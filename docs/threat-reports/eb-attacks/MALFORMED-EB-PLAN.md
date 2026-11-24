# Malformed-EB pen-test family — encoding-level invalid EBs (T11)

**Status:** proposed, ordered for execution. Awaiting review, then implement.

**Threat coverage:** T11 *"Create invalid EB"* (Inaction and Nuisance), the
encoding half the existing fake-EB family does not touch. Companion to
[`PLAN.md`](./PLAN.md) (the Phantom / Dummy / Mega *content* family) and to the
T11 red-team notes in [`../docs/t11-t16-redteam.md`](../docs/t11-t16-redteam.md).

---

## Why this exists — the gap

Everything in the fake-EB family today emits **valid CBOR** and announces the
`blake2b_256` hash of those valid bytes. What the existing variants vary is never
the wire encoding:

| family | variants | what it alters | leaves the encoding … |
|---|---|---|---|
| fake-EB (content) | `phantom-tx-eb`, `dummy-tx-eb`, `loaded` | the **referenced tx set** | valid |
| size-lie (declared) | `hollow-eb`, `lie-about-eb-size`, offer/announce `EbSizePolicy` | the **advertised** `eb_size` only | valid |

Legend: EB = Endorser Block; RB = Ranking Block (the Praos block); manifest = the
CBOR EB body, a CIP-0164 `{ tx_hash => tx_size }` map (encoded by
`leios-tools/net-rs/net-codec/src/eb.rs::encode_overflow_eb`); declared/advertised size = the `eb_size` in
the header `announced_eb` commitment or in `MsgLeiosBlockOffer`, not the real
body bytes; `EbSizePolicy` = the outbound size-rewrite policy in
`leios-tools/shared-rs/consensus/src/behaviour/tree/control.rs`.

No variant corrupts the EB's actual encoding. `phantom` / `dummy` / `loaded`
change *which txs* are referenced; `hollow` and the size-lies change the *number
advertised*. All route through
`leios-tools/net-rs/net-codec/src/eb.rs::encode_overflow_eb` and announce the
hash of its clean output. **Zero-byte body, actually-oversized body, malformed /
truncated CBOR, and decodable-but-invalid-crypto EBs are all unbuilt.** Mega
(oversized) is documented in [`PLAN.md`](./PLAN.md) as planned only.

---

## The design insight — three rejection stages

T11 explicitly asks us to measure **at which stage** an invalid EB is rejected,
because wasted work differs by an order of magnitude. Because the announced point
hash is `blake2b_256(eb_data)`, a receiver content-addresses the body **before**
decoding it. That yields three distinct targets. A malformed variant selects one
by deciding whether the announced hash matches the corrupted bytes:

| stage | gate on the receiver | how a variant lands here | wasted work |
|---|---|---|---|
| 0 | **content-address / hash** — recompute `blake2b_256(body)` vs the committed point hash | corrupt bytes *after* hashing (announce hash of clean bytes, serve garbage) | ~none (dropped pre-decode) |
| 1 | **CBOR decode** | announce the hash *of* the corrupted bytes so the hash gate passes, then fail the decoder | minimal |
| 2 | **cryptographic verification** (VRF / body signature) | a *decodable* EB: valid CBOR envelope, bad VRF or zeroed body signature | significant |

Legend: content-address gate = the `blake2b_256`-of-fetched-body check that also
root-caused the relay churn in `docs/block-production-sitrep.md`; decode =
`net-codec` CBOR parse; crypto verification = VRF-lottery and body-signature
checks a real validator runs after a clean decode.

Stage 2 only bites against a validator that actually runs those checks. A
net-rs-only cluster does not (the Dummy finding established net-rs voting is
timing-only). **proto-devnet does** — the Haskell nodes decode and validate — so
it is the correct initial vehicle for all three stages.

---

## Test environment — proto-devnet is the primary vehicle

net-rs **sources** the attack (produces the malformed EB on its own RBs); the
three honest **Haskell** nodes in proto-devnet **receive, decode, and validate**
it. This is far more efficient than the live Leios testnet or Ignite for initial
work, and unlike a net-rs-only cluster it exercises the real decode and crypto
stages. Testnet / Ignite remain a later confirmation step, not the first one.

- **Launch:** `make reset && make start` (fresh devnet, per the smoke integrity
  mandate), then arm the attack via `POST /api/attack` or net-ui.
- **Observe on the Haskell side:** the receiving node's `node.log`
  (`LeiosKernel` traces: `AnnouncementAccepted`, `EbValidated`, `NotVoted` with
  its `reason`), plus the fetch-then-reject work (fetch requests, bytes) the EB
  induces before rejection.
- **Observe on the net-rs side:** `net-rs/logs/node-0.log` (the
  `behaviour: announcing … EB` trace per variant) and `GET /api/sensors`
  (`wrong_eb_rate`, `vote_decline_rate`, `missing_tx_rate`,
  `eb_quorum_deficit`).
- **Reach is stake-bounded:** every variant fires only on the adversary's own
  produced RBs, exactly like the existing fake-EB family.

---

## The seam — files each variant touches

The wire path is fully mapped. A new variant is a fixed set of edits, two repos:

1. **`leios-tools/shared-rs/consensus/src/behaviour/registry.rs`** — add an
   `ActionSpec` variant with its `#[serde(rename = "<kebab-id>")]` tag + fields
   (cf. `phantom-tx-eb` at `registry.rs:67`, `dummy-tx-eb:73`, `hollow-eb:81`).
2. **`.../behaviour/tree/actions.rs`** — add the dispatch arm
   `ActionSpec::X { .. } => Box::new(catalogue::X::new(..))` (cf. `:106–109`).
3. **`.../behaviour/actions/fake_eb.rs`** — add the `LeafAction` struct that sets
   `out.praos.fake_eb = Some(FakeEbKind::X { .. })`; re-export in `actions/mod.rs`.
4. **`.../behaviour/tree/control.rs`** — add the `FakeEbKind::X { .. }` variant
   (enum at `control.rs:83`).
5. **`net-rs/net-node/src/production.rs`** — add a `match fake_eb` arm (at
   `production.rs:500`) building `ProducedEb`. The match is exhaustive by design,
   so a new variant **fails to compile until actuated** — nothing silently falls
   back to honest. Corruption post-processes `encode_overflow_eb`'s output; for a
   stage-1 hit, re-hash the corrupted bytes into the announced `point`.
6. **`behaviours/lib/<id>.bt`** (leaf) + **`behaviours/attacks/<id>.bt`**
   (wrapper), then `make behaviors` compiles them to
   `net-rs/net-cluster/behaviours/<id>.toml`, discoverable via `GET /api/attacks`
   and net-ui.

Clamp every attacker-supplied length before allocation (`clamp_fake_n_txs` in
`production.rs` is the model): an unbounded override via `/api/attack` must not
OOM the node.

---

## Ordered execution plan

Ordered lowest-risk / highest-value first. Each step is independently testable on
proto-devnet before the next begins.

### Step 1 — `malformed-eb` (stages 0 + 1)   — effort LOW

**Goal.** The cheap-reject measurement T11 asks for and nothing currently
provides: how early does a Haskell node discard a garbage EB, and how much work
does it do first?

**Knobs (`FakeEbKind::Malformed { corruption, hash_matches }`).**

- `corruption`: `truncate` (cut valid CBOR mid-body) · `garbage` (random bytes) ·
  `zero-body` (serve a genuine empty `[]` where a body is expected) · `bad-type`
  (right length, wrong CBOR major type).
- `hash_matches: bool`: `false` → announce the hash of the *clean* bytes, serve
  the corrupted body → **stage 0** (hash-gate reject). `true` → announce the hash
  *of the corrupted bytes* so the hash gate passes → **stage 1** (decode reject).

**Actuation.** Build a normal small manifest via `encode_overflow_eb`, then apply
`corruption` to the `data: Vec<u8>`. If `hash_matches`, set the announced
`point.hash = blake2b_256(corrupted)`; else keep the clean hash. `eb_bodies:
None`; serve the corrupted `data` on `LeiosFetch`.

**Behaviour trees.** `behaviours/lib/malformed-eb.bt` leaf +
`behaviours/attacks/malformed-eb.bt` wrapper; one wrapper per interesting
(`corruption`, `hash_matches`) pair, or a single leaf with parameters.

**Expected signature.** Stage 0: the receiver never logs `AnnouncementAccepted`
progressing to a body decode; the body is dropped at the hash check (the same
content-address gate as the relay-churn fix). Stage 1: a decode-error trace, no
`EbValidated`, `wrong_eb_rate` ↑. Honest EBs from the other lanes still certify —
the chain degrades, does not collapse.

**Acceptance.** On a fresh proto-devnet, each `corruption` × `hash_matches`
combination is observed rejecting at the predicted stage in the Haskell
`node.log`, with the fetch-then-reject work quantified (requests + bytes before
rejection). Isolated and repeatable; `cargo test -p net-node` green.

### Step 2 — `mega-eb` (oversized / resource bounds)   — effort MEDIUM

**Goal.** Probe robustness and allocation bounds — the resource-exhaustion half
of T11, finishing the Mega variant sketched in [`PLAN.md`](./PLAN.md). Distinct
from `hollow-eb`, which lies about the *declared* size while serving an empty
body; `mega-eb` ships an **actually** large manifest / body.

**Knobs (`FakeEbKind::Mega { n_txs, declared_bytes: Option<u64>, servable }`).**

- `n_txs` very large (real oversized manifest / body).
- `declared_bytes` oversized (composes with the `hollow` declared-size lie).
- `servable`: phantom-mega (unfetchable — probes fetch retry/bitmap + per-EB
  tracking under load) vs dummy-mega (servable — probes body transfer + storage).

**Key checks.** Per the `net-rs` security-audit rule that every wire length is
bounded before allocation: *pass* = the mega EB is rejected/capped with a clean
error and the Haskell nodes stay live; *fail* = memory spike / OOM / decode
panic. Also measure fetch amplification (work per adversary RB), slot-time /
liveness (`rb_reorg_rate`, `tip_disagreement`, `chain_growth_deficit`), and, for
dummy-mega, unbounded store growth (relates to T31).

**Safety.** Start with modest `n_txs` and ramp. Gate the extreme sizes behind an
explicit parameter so a casual net-ui launch cannot OOM the operator's machine.
Hard-clamp on the net-rs side regardless of the requested value.

**Telemetry.** Badness sensors are not enough for a robustness result: add a
lightweight per-node resource sample (peak RSS, fetch bytes, decode time) to the
run, or drive a single-victim setup for a clean measurement.

**Acceptance.** On a fresh proto-devnet, an oversized EB is rejected/capped
cleanly with the Haskell nodes staying live; the amplification factor and peak
resource use are recorded; no OOM / panic. `cargo test -p net-node` green.

### Step 3 — `invalid-crypto-eb` (stage 2)   — effort MEDIUM

**Goal.** The faithful "maximize wasted validation work" attack: a
decodable-but-invalid EB (valid CBOR, bad VRF or zeroed body signature) that
survives the hash gate and the decoder and fails only at cryptographic
verification on a real Haskell node.

**Note.** Piranha's native EB already carries a placeholder VRF and a zeroed body
signature (`production.rs`), so much of this may be exposing that existing
invalidity as a controllable leaf and confirming the stage-2 reject on
proto-devnet's Haskell validators, rather than new crypto-forging.

**Knobs (`FakeEbKind::InvalidCrypto { .. }`).** Select which check to fail: VRF
lottery vs body signature; zeroed vs wrong-but-well-formed.

**Expected signature.** The receiver logs the EB decoding cleanly, then a
`NotVoted` / rejection at the crypto stage (a reason distinct from
`chainTipDoesNotAnnounce` and from `MissingTX`); measurably more work than the
stage-1 decode reject.

**Acceptance.** On a fresh proto-devnet, the EB is observed rejected at crypto
verification (not earlier), with the extra work over stage 1 quantified. Testnet
/ Ignite confirmation is a follow-up, not part of this step.

---

## Sensors and measurement (shared)

- `wrong_eb_rate` — rises for stages 1–2 (malformed / invalid EB seen).
- `missing_tx_rate` — stays ~0 (these are encoding defects, not missing txs);
  distinguishes this family from Phantom.
- `vote_decline_rate`, `eb_quorum_deficit` — the fake EBs fail; honest EBs from
  the other lanes still certify, so the chain degrades rather than collapses.
- **Stage attribution** comes from the Haskell `node.log`, not sensors alone:
  which trace the EB reaches (hash drop → no accept; decode error; crypto-stage
  `NotVoted`) is the T11 result.
- **Work quantification** — fetch requests + bytes and, for `mega-eb`, peak RSS /
  decode time before rejection: the "wasted work" T11 is fundamentally about.

---

## Status summary

| step | variant | consensus leaf | net-node actuation | BT config | verified (proto-devnet) |
|---|---|---|---|---|---|
| 1 | `malformed-eb` (stages 0+1) | 📋 | 📋 | 📋 | 📋 |
| 2 | `mega-eb` (oversized) | 📋 | 📋 | 📋 | 📋 |
| 3 | `invalid-crypto-eb` (stage 2) | 📋 | 📋 | 📋 | 📋 |

Legend: ✅ done · ⏳ in progress · 📋 planned. "consensus leaf" =
`leios-tools` registry/action/control wiring; "net-node actuation" = the
`production.rs` match arm; "BT config" = the `.bt` leaf + wrapper compiled by
`make behaviors`.
