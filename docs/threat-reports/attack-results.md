# Leios Adversarial — Attack Results

Living tracker of the attacks we run and how they fare in each environment. The
summary table is the at-a-glance index (grouped into categories); each attack has
a detailed section below, under its category, recording methodology, data, and
interpretation.

**How to extend:** add a row under the right category in the summary table, and a
matching `###` section (named exactly as the row) under that category's `##`
heading. Update an environment cell from `—` to a verdict when that environment is
tested. **Category headers are `**=== Name ===**` rows (not attacks); introduce a
new category with that format.**

## Summary

| Attack | Simulation | net-cluster | Ignite | Testnet |
|---|---|---|---|---|
| **=== Penetration Tests ===** | | | | |
| Phantom Tx EB | — | negative | — | — |
| Dummy Tx EB | — | positive | — | positive |
| Mega Tx EB | — | — | — | — |
| EB Size Lie | — | — | — | positive |
| **=== Consensus & Censorship ===** | | | | |
| Reorg-Amplified Cert Suppression | — | negative | — | — |

**Legend — environments** (increasing fidelity / decreasing control):
- **Simulation** — `sim-rs` discrete-event Leios simulator (idealized, fast, no real networking).
- **net-cluster** — `net-rs` local multi-node cluster of real `net-node` processes over real
  mini-protocols (this repo's harness; the one used below).
- **Ignite** — larger/managed local-or-remote Leios devnet deployment.
- **Testnet** — the shared Leios testnet (notably Haskell nodes → cross-implementation checks).

**Legend — verdicts:**
- **positive** — the attack achieved its goal (exploit reproduced / measurable disproportionate harm).
- **negative** — the attack did **not** achieve its goal in that environment (defended, or effect
  proportional/below-threshold/at noise floor).
- **partial** — mixed or inconclusive-but-notable (see the section).
- **—** — not yet tested in that environment.

## Methodology & Parameters

Common setup for the net-cluster experiments (a section notes any deviation):

- **Cluster config:** `net-cluster/configs/sample-cluster-leios-baseline-50.toml` — 50 nodes,
  `degree = 5`, `mainnet-shaped` stake, release build. (All three experiments below now use this
  50-node config; the reorg section notes where its numbers refine the earlier 30-node run.)
- **Valid stake range: < 25% only.** It is already established that ≥ 25% adversarial stake can deny
  Leios operation, so a "success" at ≥ 25% is not a finding — sub-25% is where the interesting
  exploits live. Any higher-stake run is a *sensor/mechanism control*, flagged as such, never a verdict.
- **Stake granularity:** `stake-fraction` selection rounds **up to whole nodes**. `mainnet-shaped`
  makes ~70% of nodes zero-stake relays; the rest are equal pools — so 50 nodes ⇒ 14 pools at **~7.2%**
  each, giving distinct sub-25% points **7.2% / 14.4% / 21.6%** (1 / 2 / 3 pools; requests
  `0.05 / 0.10 / 0.15`). (30 nodes gave only ~11% / ~22%.)
- **Clean chains:** each stake point runs from a **fresh cluster restart** (restart-per-config) —
  reorg/churn state persists across hot-swaps and contaminates results.
- **Measurement:** the reading is the tail-average of per-slot sensor snapshots once the ~20-EB
  certification window has filled (`eb_quorum_deficit` samples ≥ 14). Sensors are windowed (~20 EBs),
  so effects producing ≤ 1 event/window are at the noise floor. The fixed cluster seed makes runs
  largely deterministic (identical configs reproduce identical readings).
- **50-node baseline is mildly churny** even honestly (`missing_tx_rate` ~0.09, `eb_quorum_deficit`
  ~0.10) from the larger network diameter — read attack effects as deltas above baseline.

---

## Penetration Tests

The fake-EB family — probe how honest nodes handle EBs that reference transactions never
legitimately in any mempool. Full design (variants A/B/C) in [`eb-attacks/PLAN.md`](eb-attacks/PLAN.md). The rows
below are **placeholders**: no results are logged under the results methodology yet. Where a
preliminary net-cluster observation exists from earlier ad-hoc runs, it is noted as *context only*,
not as a table verdict.

### Phantom Tx EB

**Verdict:** net-cluster = **negative** (2026-07-21, 50-node). Design: [`eb-attacks/PLAN.md`](eb-attacks/PLAN.md)
(variant A). BT: `behaviours/attacks/phantom-tx-eb.bt`.

**Goal.** Announce an EB whose manifest is `n_txs` nonexistent tx hashes, pinning **no** bodies.
Honest voters fetch the EB, fail to fetch its txs, and should decline `MissingTX`; the fake EB should
never reach quorum. Probes missing-tx handling + fetch resource-waste.

**Headline.** The network defends correctly. Phantom EBs **never certify** at any tested stake
(`fabricated_eb_certified_rate = 0` throughout); the only effect is proportional decline-churn —
`missing_tx_rate` rises with stake (then plateaus) as honest voters repeatedly fail to fetch the
phantom txs. No certification denial (`eb_quorum_deficit` stays near baseline), no soundness breach.

| Adversary stake (pools) | `fabricated_eb_certified_rate` | `missing_tx_rate` | `eb_quorum_deficit` |
|---|---|---|---|
| baseline (0) | 0.00 | 0.09 | 0.10 |
| 7.2% (1) | 0.00 | 0.27 | 0.04 |
| 14.4% (2) | 0.00 | 0.44 | 0.10 |
| 21.6% (3) | 0.00 | 0.44 | 0.11 |

Legend: `fabricated_eb_certified_rate` = fraction of the adversary's fabricated EBs that reached
quorum (0 = all rejected); `missing_tx_rate` = fraction of committee vote decisions declining
`MissingTX` (baseline ~0.09 is honest diffusion churn); `eb_quorum_deficit` = shortfall of EBs
reaching quorum. All stakes < 25% (valid range); values are seed-deterministic (the 14%/22% rows
coincide because the effect plateaus).

Interpretation: phantom is a resource-waste / missing-tx-handling probe, and the handling is correct —
unfetchable EBs are declined and never certified. Harm is bounded and proportional (the adversary's
own ~stake-share of EB slots produce non-certifying EBs; honest voters waste fetch attempts). Not a
viable exploit below 25%. Testnet/Haskell behaviour expected to match (missing-tx handling is not
implementation-specific).

### Dummy Tx EB

**Verdict:** net-cluster = **positive** (2026-07-21, 50-node) — a soundness gap; Testnet = **positive**
(2026-09-03, real musashi) — **crashed live IOG committee nodes** (see the 2026-09-03 re-test below,
which supersedes the 2026-08-27 negative). Design: [`eb-attacks/PLAN.md`](eb-attacks/PLAN.md) (variant B). BT:
`behaviours/attacks/dummy-tx-eb.bt`. Sensor: `fabricated_eb_certified_rate`.

**Goal.** Announce an EB of `n_txs` fabricated txs whose bodies the adversary pins (servable), so
honest voters clear the `MissingTX` gate. Probes **soundness**: does anything reject an
available-but-fabricated EB, or does it reach quorum?

**Headline.** It reaches quorum. **Every** fabricated Dummy EB was accepted by the committee
(`fabricated_eb_certified_rate = 1.0` at all tested stakes), with `missing_tx_rate ≈ 0` (the dummy txs
are servable, so the availability gate passes) and honest certification otherwise healthy
(`eb_quorum_deficit ≈ 0`). The committee certifies content that was never in any honest mempool.

| Adversary stake (pools) | `fabricated_eb_certified_rate` | `missing_tx_rate` | `eb_quorum_deficit` |
|---|---|---|---|
| 7.2% (1) | 1.00 | 0.11 | 0.09 |
| 14.4% (2) | 1.00 | 0.00 | 0.00 |
| 21.6% (3) | 1.00 | 0.00 | 0.00 |

Legend: as above; `fabricated_eb_certified_rate = 1.0` = all of the adversary's fabricated EBs reached
quorum. All stakes < 25%.

Interpretation: this is a per-EB **soundness** property, not a stake-threshold effect — each dummy EB
the adversary produces certifies, at any stake. The cause is that net-node's voting path performs **no
ledger-level tx-content validation** (it checks availability via `MissingTX`, not validity), so
servable-but-fabricated txs pass. **Caveat / cross-check:** this is a net-cluster (net-node) result;
it does not by itself prove a live vulnerability. The decisive test is **Testnet against Haskell
nodes**, which *do* validate tx content — a validating voter should reject the dummy txs and drive
`fabricated_eb_certified_rate → 0`. That net-cluster-vs-Haskell differential is the real finding this
sets up.

**Testnet cross-check — negative (2026-08-27, real musashi, PIR0 vs the live network).** Ran via
`POST /api/attack {"id":"dummy-tx-eb"}` against our single real forging node (peered only through the
local musashi-relay, per the internal node-ops runbook), disarmed immediately after one
firing. Fired at slot 1804667 (block 80923, EB hash `81bf8d82…`, 8 fabricated servable txs, identical
mechanism to the net-cluster run). **Did not certify.** Six subsequent real blocks (80924–80929,
issued by four different real pools, spanning 4+ minutes / 250+ slots — well past the
`stage_length_slots = 20` voting window) all show `body_cert_eb_hash=none`, `eb_certified=false`; no
`RB applied with cert` event for this EB hash ever appears, in sharp contrast to the steady stream of
real certifications running every few minutes right up until announcement. Independently corroborated
via the real relay's own `journalctl -u musashi-relay` `ChainDB.AddBlockEvent` log (not net-rs's
self-report) for blocks 80923–80929. This is the opposite result from net-cluster, consistent with the
predicted differential: the real network did not certify content it could not validate. Caveat: we can
observe only the *absence* of a certification, not the real peers' internal decline reasoning (no
`MissingTX`-equivalent message is visible to us the way it would be from net-rs's own simulated
voters) — the leading hypothesis (a real ledger-validating voter rejecting txs whose referenced UTxOs
don't exist) is plausible and consistent with all evidence but not directly confirmed by log content.

**Testnet re-test — positive (2026-09-03, real musashi, PIR0 → local relay → live committee): crashed
live IOG committee nodes.** The 2026-08-27 "negative" above was a **false negative caused by a
diffusion gap, not by real validation rejecting the content** — the fabricated EB's body never reached
the committee at all.

- **Root cause of the false negative.** net-node announced the fake EB only as an on-chain reference
  (the RB it forges, diffused via ChainSync) and gossiped the availability *announcement*, but **never
  made the LeiosNotify body/tx offer** (`MsgLeiosBlockOffer` / `MsgLeiosBlockTxsOffer`) for its own
  self-produced EB. LeiosFetch is **offer-driven**: a peer issues a `BlockRequest`/`BlockTxsRequest`
  only for a body another peer has *offered*. With no offer, our upstream relay counted the EB among
  `missingBodies` but issued **zero** fetch requests, never obtained the body, and never re-diffused it
  — the poisoned txs stalled one hop short of the committee. Confirmed on the relay:
  `Consensus.LeiosKernel.AnnouncementAccepted source=receivedViaChainSync`, `FetchDecision` with
  `missingBodies>0` but `decisionBodyRequests=0`, and **zero** `BlockOffer`/`BlockTxsOffer` for any
  fabricated hash.

- **Fix (`net-node` only; leios-tools untouched).** `production.rs`: `ProducedEb` gains
  `eb_bodies: Option<Vec<TxBody>>`; the Dummy branch fabricates the `n_txs` bodies once and carries
  them (Phantom stays `None` — unfetchable by design; honest EBs stay `None` and resolve from the
  mempool). `main.rs`: the self-produced-EB handler injects the carried bodies straight into the
  `LeiosStore` via `inject_block_txs_full`, which enqueues the `BlockTxsOffer` and makes the txs
  fetch-servable — the producer now **makes the offer** instead of relying on a mempool re-resolution
  that never succeeds for fabricated txs (the old path logged `EB will be unfetchable`). Unit test:
  `dummy_fake_eb_carries_servable_bodies_matching_manifest`.

- **End-to-end on musashi** (attack armed live via `POST /api/attack {"id":"dummy-tx-eb",
  "selection":{"kind":"all"}}` — a behaviour-tree swap, no node restart). For each fabricated EB
  (e.g. slot 2342922, EB `fe57ed2c…`, 8 dummy txs, 297-byte body): net-node logs `announcing DUMMY Tx
  EB` → `offered self-produced fake-EB bodies for LeiosFetch serving n=8` → `EB offered` +
  `EB txs offered` (no `will be unfetchable`). The relay then, for the first time, **fetches from us**
  — `LeiosFetch.Send.BlockRequest` + `Send.BlockTxsRequest (numTxs=8)` → we serve the 297-byte body
  and all 8 txs → relay `BlockAcquired` + `BlockTxsAcquired` + `BodyHits` → relay **re-offers and
  serves onward** (`Send.Block ×21`, `Send.BlockTxs ×19`). The offer→fetch→serve→re-diffuse chain that
  was broken now completes.

- **Delivery to the IOG pools.** IOG1 / IOG2 / IOG3 share one registered relay,
  `leios-node.play.dev.cardano.org:3001` (also our relay's configured upstream peer). For **all four**
  fabricated EBs of the run (`a9d65290…`, `b80dfb4f…`, `f72ed98f…`, `fe57ed2c…`) our relay served
  **both the EB body and the 8 fabricated txs** to that relay — observed as
  `LeiosFetch.Remote.Send.Block` + `Send.BlockTxs` to that relay's hostname
  on the exact registered port, pull-initiated by the IOG relay. What our vantage proves is delivery
  to the IOG *relay* fronting the three block producers; the final relay→BP hop is inside IOG's network.

- **Impact — live crash.** Deserializing the fabricated txs **crashed several live IOG committee
  nodes** (confirmed by the SRE team; the `Leios.hs:283` CBOR-deserialize path). The run was
  pre-coordinated with SRE, disarmed via the attack API immediately after confirmation, and the
  affected nodes were restored; the offending CBOR snippet was shared with the developers to fix.

**Interpretation.** This **supersedes** the earlier negative. The real Haskell implementation does not
safely reject the available-but-fabricated content — it **crashes** on deserialising it, a strictly
more severe outcome than the predicted validation-reject, and reachable by a **sub-25% adversary
through a single honest relay**. The earlier "does the real network validate content?" question is
moot: the node never gets far enough to validate, because the deserialiser faults first. The net-rs
offer fix is what made the path reachable end-to-end on the real network; without it the poison never
leaves the adversary's first hop.

### Mega Tx EB

**Verdict:** all environments untested (placeholder — not yet implemented). Design:
[`eb-attacks/PLAN.md`](eb-attacks/PLAN.md) (variant C).

**Goal.** Announce an EB with an enormous manifest (`n_txs` very large) and/or an oversized declared
size, forcing every committee member to fetch/decode a giant EB — an asymmetric resource / DoS probe
of allocation bounds and fetch amplification (net-rs security-audit rule 1).

### EB Size Lie

**Verdict:** Testnet = **positive** (2026-08-27, real musashi) — confirmed real-network connection
disruption. net-cluster untested. Existing offer-path machinery: `leios.offer_eb_size` policy (the
`lie-about-eb-size` action, `server_handlers.rs`). Announce-path counterpart (the `announced_eb` size
field on the RB header, `announce-size-lie` in `production.rs`) **is implemented** (the note below
about it being future work is stale — it was live and fireable at time of test). Combined attack:
`behaviours/attacks/dummy-tx-eb-size-lie.bt` (Join of `dummy-tx-eb` + `announce-size-lie`).

**Goal.** Advertise an EB whose **declared byte size** disagrees with its true body size, and see how
honest peers handle the mismatch. This is not a tx-content attack (unlike Phantom/Dummy/Mega) — the
EB body may be entirely honest; only the advertised `eb_size` is fabricated. CIP-0164 requires the
size so a peer can **pre-size its fetch buffer**, which is exactly what the lie targets:

- **Under-declare** (claim a large EB is small) — confirmed below to make the real relay **drop the
  connection**, at both the degenerate (`eb_size = 0`) and non-degenerate (~1/10 true size) points.
- **Over-declare** (claim a small EB is huge) — confirmed below to produce the **identical** connection
  drop (true size + 10,000 bytes on a genuinely empty EB). The effect is direction-independent — see
  "Revised headline" below.

**Surfaces (two, same lie):** the `MsgLeiosBlockOffer.eb_size` field (offer path, implemented via
`lie-about-eb-size`) and the `announced_eb = (hash, size)` field carried in the RB header on the fast
announce pulse (`MsgLeiosBlockAnnouncement`, announce path, implemented via `announce-size-lie`). The
announce-path variant lands the lie *earlier* (within the Δhdr discovery budget), so it can reach a
peer before the honest offer would. The result below exercises the **announce path only**.

**Testnet result — positive, both under-declare variants tested (2026-08-27, real musashi).** Real
content (8 servable dummy txs, same generator as the Dummy Tx EB test above) announced via a single
real forging node with a false `announced_eb` size baked into the RB header, fired via
`POST /api/attack {"id":"dummy-tx-eb-size-lie", "parameters": [...]}`, disarmed immediately after each
firing:

| Variant | Declared vs true size | Slot / block | Effect |
|---|---|---|---|
| Degenerate zero | `scale_num=0` → declares 0 | 1806416 / 80988 | Hard disconnect, ~15ms after announcement |
| Under-declare | `scale_num=1, scale_den=10` → declares ~1/10 true | 1806833 / 81014 | Hard disconnect, ~15ms after announcement |

Both firings produced the **identical** signature on the real relay connection:
`ERROR net_core::mux: mux shutdown: demuxer error: I/O error: Connection reset by peer (os error 104)`,
immediately followed by `peer disconnected ... reason=leios_notify: mux error: mux shut down`. Neither
EB certified (the connection reset happened before any vote/cert cycle could complete). Recovery was
automatic and clean in both cases — a fresh connection formed within seconds and net-rs resynced to
the real tip with no lasting damage.

**Headline.** This is a broader finding than the previously-known degenerate case: the real relay does
not merely special-case `eb_size = 0` — a **10× under-declaration with a nonzero, plausible-looking
value** produced the exact same hard reset, at the same ~15ms latency. That's more consistent with the
real implementation strictly validating declared-vs-actual size (or byte framing derived from it) than
with a special-cased zero check. This also settles the "no consumer" question raised during testing
([`docs/eb-announcement.md`](docs/eb-announcement.md) §4 states the announce-path attacks are "forward-looking... nothing
downstream consumes an announcement for validity yet" for *this project's own* net-core — that framing
does not extend to the real cardano-node prototype, which clearly does react).

**Third data point — honest content, same result (2026-08-27, real musashi).** Repeated the 10%
under-declare with `announce-size-lie` armed **alone** (no `dummy-tx-eb`) against an honest EB: a
freshly-built self-transfer magazine (3 real, honestly-signed txs, `size-lie-honest.mag`) loaded as
`tx_source`, same EB-only knobs (`provide_txs=false`, `rb_body_max_bytes=0`). Fired at slot 1807374
(block 81039): `produced endorser block ... fabricated=false`, `Inserting (locally produced) manifest
... with 3 txs (3 ours, 0 non-ours)` — genuinely honest content, not fabricated. The identical
connection-reset signature fired anyway, ~11ms after announcement. `settled.sh` afterward confirmed
0/3 txs landed. **This rules out fabricated content as a confound**: the effect is purely a function of
the declared/actual size mismatch, independent of whether the content is honest or fabricated — a
stronger and more general finding than the first two data points alone established. The 3 real txs
remain unsettled in `size-lie-honest.mag` (cursor not yet consumed) for a future honest re-run.

**Fourth data point — over-declare also triggers it (2026-08-27, real musashi).** Ran the planned
empty-manifest composition, `behaviours/attacks/empty-eb-size-lie.bt`
(`Join[phantom-tx-eb(n_txs=0), announce-size-lie(offset=10000)]`): a genuinely empty EB (true size 1
byte, CBOR `a0`) announced with a declared size ~10,000 bytes larger — the **over**-declare direction,
untested until now. Fired at slot 1807822 (block 81053, EB hash `d36a2619…`). This is the one case
that needed two false starts to test correctly: the first two arm attempts coincided with ambient
relay reconnect churn that was **not actually caused by the attack** (confirmed by checking that
`dynamic config updated` — proof the behaviour tree change had reached the running node — postdated
the disconnect, and that no `fabricated=true` production ever occurred in either window); both were
incorrectly reported as results before this was caught and corrected. The third attempt was watched by
triggering specifically on `fabricated=true` rather than on any disconnect, giving an unambiguous
production event to anchor to. On genuine firing, the same connection-reset signature appeared, ~11ms
after announcement, but this time with every multiplexed sub-protocol's teardown visible in one place
(`chainsync`, `blockfetch`, `leios_fetch`, `peersharing` all logged `recv error, exiting ... mux shut
down` against the same `peer-7` we'd just served the announcement to) — confirming this is a
**connection/mux-level** reset, not specific to the `leios_notify` channel. (This was itself first
mis-read as "no effect" from an incomplete grep that searched only for the EB's hash, which doesn't
appear in the disconnect lines; corrected on a broader search.)

**Revised headline: the effect is not direction-specific.** Under-declaring (0 or 1/10 of true size,
fabricated or honest content) and over-declaring (true size + 10,000 bytes, empty content) all produce
the identical immediate hard reset. The real relay appears to reset the connection on **any**
declared/actual size mismatch in the RB header's announcement, not specifically on promising less than
it delivers. Four consistent data points now (size=0/fabricated, 1-10×-under/fabricated,
1-10×-under/honest, over-declare/empty), zero exceptions.

**Caveats.** (1) None of the declared values were independently verified by decoding the raw header
bytes — the evidence each applied is that its `/api/attack` request validated and queued without error
(an unknown-parameter or type-mismatch would have been rejected with 400); the
`GET /api/attacks/:id/parameters` `current_value` field appears to reflect the compiled file's static
defaults, not live runtime overrides, so it could not be used to confirm this. (2) We cannot see the
real relay's internal reasoning, only the connection-level effect — whether this is a deliberate
validation rule or an unrelated parsing bug that happens to manifest as a hard reset is not
distinguished by this evidence. (3) Four data points only; the exact boundary (how small a mismatch
still triggers it?) is untested. (4) **Methodological note for future tests on this attack
specifically:** verify causation by triggering on the attack's own production log line
(`fabricated=true`, or an equivalent unambiguous marker of the armed behaviour actually firing) before
concluding anything from a nearby disconnect or its absence — ambient relay reconnect churn occurs
independent of any attack and produces an identical-looking signature; a temporally-close disconnect,
or the absence of one, is not by itself evidence about this attack without confirming the production
event actually occurred first.

---

## Consensus & Censorship

### Reorg-Amplified Cert Suppression

**Verdict:** net-cluster = **negative** (refreshed 2026-07-21 on the 50-node config; supersedes the
earlier 30-node run). Related bead: `p89.2`. Attack BT: `Join[cert-suppressor, deep-reorg]` on the
selected adversary node(s).

**Goal.** Find a minimum-stake (< 25%) attack that *disproportionately* blocks EB certification —
i.e. leverage `= cert_suppression_rate / stake > 1` — by using `deep-reorg` to manufacture the "child
of a quorum'd EB" position that `cert-suppressor` needs (strict parent-only cert rule).

**Headline.** No. (1) Plain cert-suppression is at best **proportional** (leverage ≈ 0.9 at the 50%
control; at sub-25% stake it sits at the ~1-EB/window noise floor) — never disproportionate. (2) The
reorg does **not** amplify — it *destroys* the plain suppression (drops it to 0) while adding heavy
chain churn. The `p89.2` "disproportionate sub-25%" hypothesis is refuted.

#### Plain cert-suppressor — stake sweep (50-node, clean restart-per-config)

| Adversary stake (pools) | `cert_suppression_rate` | leverage† |
|---|---|---|
| 0 (baseline) | 0.00 | — |
| 7.1% (1) | 0.076 | 1.06 |
| 14.3% (2) | 0.076 | 0.53 |
| 21.4% (3) | 0.076 | 0.35 |
| 50% (7) — *control, > 25%* | 0.455 | 0.91 |

†`leverage = cert_suppression_rate / actual_stake` (>1 = disproportionate). `cert_suppression_rate` =
fraction of quorum-reached EBs whose cert an adversary dropped (attributed, from the ground-truth
`CertSuppressed` event). The 50% row is a **sensor/mechanism control** (> 25%, *not* a valid attack
result); it confirms the mechanism scales ~proportionally (leverage ≈ 0.9). The sub-25% rows show a
flat `cert_suppression_rate ≈ 0.076` (~1 suppressed EB per ~14-EB window, seed-deterministic):
absolute suppression doesn't grow with added pools, so real leverage *falls* with stake — proportional
at best, at the noise floor. **Refines the earlier 30-node run**, which under-measured on its coarse
topology (reported leverage 0.32 at 50%); the finer 50-node topology gives ≈ 0.9.

#### Reorg-amplified — depth × stake grid (50-node)

`cert_suppression_rate` (attributed) at each reorg depth and stake, with `rb_reorg_rate` in
parentheses:

| depth \ stake | 7.1% (1 pool) | 14.3% (2 pools) | 21.4% (3 pools) |
|---|---|---|---|
| d = 1 | 0.00 (13.5) | 0.00 (55.2) | 0.00 (69.9) |
| d = 2 | 0.00 (10.5) | 0.00 (23.1) | 0.00 (32.4) |
| d = 3 | 0.00 (9.3) | 0.00 (17.0) | 0.00 (23.4) |
| d = 4 | 0.00 (7.8) | 0.00 (10.9) | 0.00 (26.1) |

Legend: each cell is `cert_suppression_rate` with `rb_reorg_rate` in parentheses. `depth` = reorg
depth (RBs rolled back per `deep-reorg`); `cert_suppression_rate` = fraction of quorum-reached EBs
whose cert an adversary dropped (0 = none); `rb_reorg_rate` = observed RB reorgs (churn magnitude).
Column stakes 7.1% / 14.3% / 21.4% = 1 / 2 / 3 pools (all < 25%, the valid range).

`cert_suppression_rate = 0` across the **entire** grid — zero attributed suppression at every depth
(1–4) and every sub-25% stake, while churn (`rb_reorg_rate`) runs 8–70 and rises with both stake and
depth. Versus plain cert-suppression's 0.076, the reorg **destroys** the suppression rather than
amplifying it: the periodic `deep-reorg` orphans the adversary's own cert-suppressing RBs (and, at
greater depth, the target EBs themselves). There is **no sweet-spot depth** — depth 3 behaves exactly
like its neighbours. Amplification would require a *targeted* reorg (fire depth-1 exactly at a quorum'd
EB's following slot), not a periodic `every_slots` one.

#### What the attributed sensor proved

Under the reorg the chain churns hard (`rb_reorg_rate` 13–76) and `eb_quorum_deficit` rises to ~0.14,
which naively reads as "the attack works". But the **attributed** `cert_suppression_rate` = **0** shows
the damage is blunt reorg **churn** orphaning EBs, not the targeted parent-only-cert exploit.
Separating targeted exploit from generic churn is exactly why `cert_suppression_rate` was built; it
reads 0 honestly and scales ~proportionally with stake (0 → 0.455 as stake 0 → 50%).

#### Why the reorg can't amplify (mechanism)

To help, a reorg must install the adversary's RB as the *immediate child of a specific quorum'd
EB-RB*, within the cert-inclusion window, while preserving that EB's own RB. The `deep-reorg` leaf is
**periodic and untargeted** (`every_slots` self-reorg), uncorrelated with quorum'd EBs, so it never
reliably manufactures that position — it just destabilizes the chain. Amplification would require a
**targeted reorg** (fire depth-1 exactly at a quorum'd EB's following slot), which is not built.

#### Methodology notes / caveats

- **`stake-fraction` rounds up to whole nodes** — on the 50-node config pools are ~7.15% each, so
  requests `0.05 / 0.10 / 0.15` resolve to 1 / 2 / 3 pools = 7.1% / 14.3% / 21.4% (all < 25%); the
  control `0.50` = 7 pools. See the shared Methodology & Parameters section.
- **Hot-swap contaminates reorg runs** (churn persists across configs); restart-per-config was required.
  An early hot-swap reading of "0.84 leverage" was an artifact and is superseded by the clean data.
- The sub-25% signal is at the noise floor (~1 suppressed EB per ~14-EB window), so individual points
  are seed-deterministic and coarse; the 50% control is the reliable proportionality anchor.

#### Recommendations / next steps

- **Reframe `p89.2`**: cert-suppression is ~proportional (leverage ≈ 0.9 at 50%), not disproportionate —
  no sub-25% advantage; and the reorg backfires (destroys the plain suppression).
- For a real low-stake attempt: build a **targeted-reorg leaf** (reorg conditioned on a quorum'd EB at
  the tip, depth = 1) + a **cumulative** suppression counter to clear the low-stake noise floor.
- **Ignite / Testnet**: untested. The Haskell-node cross-check is most interesting for the *fake-eb*
  family (soundness), less so here (this attack is stake-gated regardless of implementation).
