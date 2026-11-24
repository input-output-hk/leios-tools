# T27 — Coordinated two-sided TX flood (live Leios testnet)

**Goal:** two participants, on opposite sides of the world, each flood *valid but
disjoint* transactions into their own friendly Haskell Leios relay. If the two
tx sets can't cross fast enough, each side's Endorser Blocks reference txs the
other side hasn't fetched — so the far side **cannot certify** them. That is a
**T27** condition: the network behaves as if it were partitioned (or as if an
adversary split the txs), and **EB certification degrades**.

This document records the end-to-end design of that live attack. It reuses the
tx-injection tooling described in [`../tx-injection/`](../tx-injection/) and the
local-injection recipe in
[`../tx-injection/local-node-injection.md`](../tx-injection/local-node-injection.md).
(The operational runbook and tooling itself are internal and not published.)

---

## Why this works (the mechanism)

In Leios an **EB is a manifest of tx *hashes***, not the tx bodies. To **vote for
/ certify** an EB, a committee node must first **fetch the referenced tx bodies**
(the `LeiosFetch` protocol) and validate them. A certificate is a BLS aggregate
over a quorum (τ = 0.75) of those votes.

So certification has a hidden dependency: **every voter must already hold every
referenced tx.** Normally txs gossip everywhere in well under a slot, so this is
free. The attack breaks that assumption:

- Each side floods **distinct, valid** txs into its local relay at high rate.
- Producers near each side build EBs from their **local** mempool.
- With enough rate and enough inter-side latency, the *other* side is still
  missing those txs when the EB's votes are due → votes fail / are late →
  **the EB doesn't reach quorum** → certification rate falls.

The observable signals on a relay are `AnnouncementAccepted` (EB seen),
`Certified` (EB reached quorum), and — the tell — `BlockPointMissing` (we were
asked about an EB whose referenced data we don't have). See **Measurement**.

### TX shape — why fan-out distinct txs

The txs must be **valid and mutually non-conflicting**, because a node only
*forwards* txs that pass basic validation; anything filtered never diffuses and
so never loads the far side. That rules out the cheap shapes:

| Shape | Fate at first honest node | Use for T27? |
|-------|---------------------------|--------------|
| duplicate (same tx ×N) | de-duplicated | no |
| conflicting (same input) | all-but-one rejected | no |
| **distinct fan-out** | **all forwarded + individually fetched** | **yes** |

Legend — *fan-out*: split one funded UTxO into W disjoint "lanes", then pre-sign a
chain of C txs per lane, so every one of the W·C txs spends a distinct output and
is independently valid. Tx size is a secondary knob: many small txs maximize the
*count* of fetch units; padding toward `maxTxSize` (16 KB) maximizes fetch
*bytes* per tx. Start with small-many.

### Keeping the two sides disjoint (critical)

If both participants sign from the **same** UTxOs, their txs are double-spends and
get filtered — the opposite of what we want. Each participant therefore floods
from **their own funded key**: an *enterprise* (payment-only, `addr_test1v…`)
address that carries no stake, so spending it never disturbs the delegated stake
reserved for block production. Each operator's flood key was funded with 500k ADA
from an unstaked reserve. Key handling is internal to the team and not part of
this publication.

---

## Prerequisites (each participant)

1. **A synced Haskell Leios relay** running the current release
   (`prototype-2026w32`) against the respun musashi net (magic 164): prebuilt
   binary from the release, wiped db, pinned config, and full sync (confirmed via
   `cardano-cli` sync progress at 100%).
2. **`net-node` built** with the txsubmission fix (leios-tools **#82**, merged).
   Without #82 the flood tears the net-node↔node connection down under load;
   with it, 0 resets.
3. **A funded flood key** for your site (above).

---

## The four steps

The attack comes up in stages so each layer is verified before adding the next.
Steps 1–3 need only your Haskell node; step 4 adds the high-speed net-rs vehicle.

### Step 1 — Make your relay trust your local net-rs

A Haskell node does **not** pull txs from an unsolicited inbound peer; it only
runs TxSubmission with peers in its topology. Add your net-node as a **trustable
local root** (an unsolicited peer connection is not enough — this is the same
finding as [`../tx-injection/local-node-injection.md`](../tx-injection/local-node-injection.md))
and restart the node to read the topology.

### Step 2 — Generate your TX magazine

Build a fan-out magazine from **your** flood key: `--width 180 × --chain-depth
10 = 1800 distinct txs`.

- Keep `width ≤ ~180` (a fan-out tx must fit `maxTxSize` 16 KB) and total
  tx-bytes **under the 500 KB mempool cap** (1800 × ~228 B ≈ 409 KB ✓) so nothing
  is capacity-rejected.
- Broadcast the fan-out parent and wait for confirmation: the W lane outputs must
  confirm before the children are valid.
- **Rebuild a fresh magazine each run** — a reused magazine's resume cursor makes
  net-node replay nothing.

### Step 3 — Flood WITHOUT net-rs (functional bring-up)

Prove the pipeline using only your Haskell node — submit the signed txs over the
local socket with `cardano-cli`, one process per tx (deliberately slow). It
confirms your keys, magazine, and node accept the flood before the high-speed
phase.

### Step 4 — Flood WITH net-rs + the TX-Flood behavior tree

Swap in the high-speed vehicle: net-node replays the magazine over N2N
TxSubmission into your (now trusting) relay. The **`tx-flood` behavior tree**
action only sets the *rate*; the *content* is your magazine (via `--tx-source`),
so it floods **valid, disjoint, must-be-fetched** txs.

- Use the **respun** musashi base config (magic 164, correct `genesis_time_unix`
  for the w36 start) — **not** the pre-respin dev config, whose genesis time
  breaks sync.
- Without the behaviour tree, net-node floods at the config `tx_rate`; with it,
  the BT `rate` (default 2000/s) drives the generator. Either way a finite
  magazine caps the flood.
- Without `--tx-source`, the BT would flood **synthetic garbage** instead — for
  T27 always supply the magazine.

---

## Measurement — is certification degrading?

### A. Your own relay (per-site view)

Read the cumulative snapshot — or the delta over a live window during the flood —
from `node.log`:

| Metric | Meaning | T27 direction |
|--------|---------|---------------|
| CERT-RATE | `Certified / AnnouncementAccepted` | **falls** |
| MISS-RATE | `BlockPointMissing / (BlockTxsAcquired + BlockPointMissing)` | **rises** |

Legend — *Certified*: EB reached the vote quorum; *AnnouncementAccepted*: EB was
announced to us; *BlockTxsAcquired*: we fetched an EB's referenced tx set;
*BlockPointMissing*: we were asked about an EB whose referenced data we lacked —
the direct disjoint-set signal. (Requires `Consensus.LeiosKernel` ≥ Info; the
pinned musashi config sets it to Debug.) Capture a **baseline before** flooding
and compare to the **flood window**.

### B. A piranha net-rs node (network view)

Point one of the other net-rs machines in the topology at the testnet as a
passive observer (zero stake, zero tx rate) and read EB announce/fetch/cert from
*its* vantage — a second, independent viewpoint on whether the two sides converge.
This is the "monitor block production from another perspective" node; it neither
floods nor forges.

---

## One-shot test sequence (solo smoke test)

The internal one-shot driver does steps 2 + 4 + measurement in one go — build
magazine → baseline → flood via net-node → flood-window measurement → report.
Run it solo to validate your whole setup before the coordinated run. It prints
on-chain adoption (lanes started) and baseline-vs-flood cert numbers. Adoption
latency is ~90 s, so use a duration ≥ 180 s.

---

## The coordinated run

1. Both operators complete steps 1–3 solo and confirm their own txs land.
2. Agree a **start time**. Each starts step 4 (or the one-shot driver) at that
   time with a **large, fresh** magazine from their **own** key.
3. Each captures a cert baseline just before, then watches the live measurement
   window on their relay; the piranha observer watches the network view.
4. Compare CERT-RATE / MISS-RATE against the solo baselines. The T27 signature is
   a **coincident drop in CERT-RATE and rise in MISS-RATE** on both sides while
   both floods run, recovering after they stop (the "partition heals").

---

## Gotchas

- **net-node not a trustable local root** → node never pulls; nothing lands (step 1).
- **Stale magazine cursor** → net-node replays nothing; build fresh each run.
- **Reused / spent source UTxO** → magazine invalid; rebuild (it queries live UTxOs).
- **Same key both sides** → conflicting txs, filtered, no T27; use separate keys.
- **Wrong base config** (pre-respin genesis time) → sync breaks.
- **Node self-exits on laptop sleep** (`DiffusionError BadConfiguration` after a
  `KeepAlive` timeout storm) → keep the host awake.
- **Total magazine tx-bytes > 500 KB mempool cap** → deep txs capacity-orphan;
  keep `width·depth·~228 B < 500 KB`.

---

## Appendix — how the flood keys were made

Minted as enterprise (unstaked) payment keys and funded from an **unstaked**
reserve UTxO, leaving staked funds untouched: each operator's key is the
enterprise (no-stake) form of a payment key, spendable yet carrying no stake.
~4M ADA remains parked in the reserve to provision the other 8 piranha nodes
when they come online. (The minting tooling and key files are internal.)
