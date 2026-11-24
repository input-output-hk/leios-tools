# T14 — Include invalid certificate in RB: TEST PLAN

> **This is a TEST PLAN only.** It proposes how we *would* test T14. It does not
> implement or run anything. No code is written, no devnet is started, no
> magazine is built, and no genesis or staking parameters are changed as part of
> this document. Every "run", "arm", "measure", and "variant" below is
> prospective. Actuation, harness wiring, and measurement are described so the
> work can be scoped and reviewed before any of it is built.

## Question

When an adversarial producer forges a ranking block (RB) that carries an
**invalid** Leios EB certificate, what does an honest node *spend* to fetch,
store, validate, and finally discard that block — and can a stream of such
blocks measurably slow honest block adoption?

T14 is a **throughput / nuisance** threat, not a safety one. Its safety twin,
T15, already asked and answered the soundness question for one certificate
variant (see below). T14 reuses the *same forged block* but asks a different
question: not "does the honest node wrongly adopt it?" (T15: no), but "how much
asymmetric work can the adversary impose per forged block before it is
rejected?"

## Relationship to T15 (the completed baseline)

T14 is largely **already exercised** by the existing `single-bls-cert` audit
that Kris (kp) built and completed for **T15**. Read
[`../t15/single-bls-cert.md`](../t15/single-bls-cert.md) first — this plan is its
prospective analogue and mirrors its section shape.

- **T15 (safety — FINAL, verdict: SOUND).** Does `verifyLeiosCert` bind the
  aggregate BLS signature to the *named* signers, or does it trust the signer
  bitfield's claimed weight? Answer: it **binds**. A certificate whose bitfield
  names the whole committee but whose aggregate is one key is rejected
  `LeiosInvalidCertificate … InvalidSignature`; **0 adopted across 3 fresh
  runs** (7 forged certs shipped, all rejected).
- **T14 (throughput — this plan).** The T15 run already shows the *shape* of the
  cost: each honest node **fetched** the block, **added it to its VolatileDB**,
  then **rejected at ledger validation**. T14's contribution is (a) the
  remaining invalid-certificate variants T15 never forged, and (b) the
  **cost / reach** measurement T15 never took because it was asking a safety
  question.

So T14's *correctness* oracle is already green for one variant (the "width lie").
This plan does **not** re-plan that variant; it references it as the completed
baseline and builds outward.

**Legend.** RB = ranking block; EB = endorser block; VolatileDB = the honest
node's on-disk store of validated-but-not-yet-immutable blocks; `verifyLeiosCert`
= the Haskell Leios ledger rule that checks an EB certificate.

## The attack

The on-chain `leios_certificate` is the CBOR 2-tuple
`[signers : bytes, aggregated_signature : bytes .size 48]`, where `signers` is an
**MSB-first** bitfield over the committee (exactly `⌈n/8⌉` bytes) and
`aggregated_signature` is the 48-byte G1 aggregate of the named members' vote
signatures **over the announcing RB's hash**. This wire contract is documented in
the module header of `net-rs/net-node/src/consensus/leios/cert.rs`
(our internal node implementation; not public) and enforced by `verifyLeiosCert`, which rejects on three distinct grounds:

- `MalformedSigners` — the bitfield length is not `⌈n/8⌉` (a plain length
  compare, before any BLS math);
- `InsufficientWeight` — the named-and-seated signers do not clear the quorum
  weight gate (after decoding the bitfield, before/around aggregate checking);
- `InvalidSignature` — the aggregate does not verify against the reconstructed
  aggregate public key of the named seats (the most expensive path: it
  reconstructs the aggregate pubkey over every named seat, then does the
  pairing check).

**Legend.** `n` = committee size = the live registered-pool count the election
drew from (the bitfield width *moves* as pools register — measured 7 bytes
pre-respin, 19 bytes at 147 pools; see the `cert.rs` header). G1 = the BLS12-381
signature group. MSB-first = bit `i` sits at byte `i/8`, bit `7-(i%8)`.

The adversary is **net-rs**, running as a ~20%-stake pool against three honest
80%-stake Haskell pools. It forges the certificate on the child RB of its
parent's announced EB (the only block that reaches `verifyLeiosCert`), and ships
it **without** the honest pre-publish self-verify drop — see "The reusable seam"
below. The honest nodes are **unmodified upstream `cardano-node`**; every reject
is theirs.

## The reusable seam (already in the codebase)

Every invalid-cert variant is a sibling of one existing function plus one control
flag plus one registry action leaf plus one `.bt` behaviour. The seam is already
proven end-to-end by T15. The code paths below live in our internal (not public)
net-rs implementation; they are cited for provenance:

- **Forgery constructor** — `net-rs/net-node/src/consensus/leios/cert.rs`:
  `build_single_bls_cert` (line 165) forges the `[signers, aggregate]` tuple with
  an all-committee bitfield over a one-key aggregate; `build_cert` (line 104) is
  the honest constructor; `all_committee_bitfield` (line 140), `build_bitfield`
  (line 69), `bitfield_len` (line 60), `bitfield_ids` (line 83) are the reusable
  bitfield helpers; `verify_cert` (line 200) and `CertVerdict` (line 185) are the
  self-check. Each new variant is a new sibling constructor here.
- **Consensus selection** — `net-rs/net-node/src/consensus/mod.rs`:
  `single_bls_certificate_for_parent` (line 503) mirrors the honest
  `certificate_for_parent` (line 485) — same certifiable-EB selection and
  self-cert gate, but substitutes the forgery. Each variant needs a sibling
  here.
- **Forge branch (the critical reusable piece)** — `net-rs/net-node/src/main.rs` lines
  ~1424–1494: armed by `control.praos.forge_single_bls_cert` (line 1431), this
  branch **deliberately skips the pre-publish self-verify drop** (lines
  1450–1465). An honest producer refuses to ship a cert it knows is bad because a
  bad cert makes the *whole RB* an `InvalidBlock` and orphans the producer's own
  chain (the drop logic at lines 1466–1494). **Every invalid-cert experiment
  needs that guard bypassed to reach the honest ledger** — this is the single
  most important piece of reuse, and it already exists.
- **Adjacent cert seams** — certificate *suppression* (`suppress_cert` /
  `cert_for_parent`, `main.rs` ~1351–1362) and the fabricated-EB / announce-flood
  family (`fake_announce`, `main.rs` ~1379–1410) are already present and are the
  models for how a new control flag threads from the BT registry to the forge
  branch.
- **Engine (this repo)** —
  [`../../../shared-rs/consensus/src/behaviour/registry.rs`](../../../shared-rs/consensus/src/behaviour/registry.rs):
  the `SingleBlsCert` action leaf and the fabricated-EB /
  suppression action family.
  `ControlSignal.praos.forge_single_bls_cert` is the control flag. Our node
  consumes this crate via a filesystem path dependency (`shared-consensus`), so
  editing it directly changes the node's build.
- **Behaviour** — `behaviours/attacks/single-bls-cert.bt` and its cluster form
  `net-cluster/behaviours/single-bls-cert.toml` are the templates (internal); each
  variant gets its own `.bt` + `.toml`.

## The variant matrix

Each row is a distinct invalid-certificate construction. The width-lie is
**DONE** by T15 and shown only as the baseline. **No code is written here** — this
table describes what each variant corrupts, which `verifyLeiosCert` ground
rejects it, and how expensive that reject is (the T14 cost axis).

| # | Variant | What it corrupts | Expected ledger reject | Reject cost (relative) | Status |
|---|---------|------------------|------------------------|------------------------|--------|
| 0 | Width lie (all-committee bitfield, one-key aggregate) | bitfield claims full quorum; aggregate is one key | `InvalidSignature` | Dear — full aggregate-pubkey reconstruction over all `n` seats | **DONE (T15)** |
| 1 | Garbage / zero aggregate | 48-byte aggregate replaced with zero / random bytes; bitfield honest | `InvalidSignature` (aggregate fails to verify; if it does not decode as a G1 point → malformed-cert reject) | Dear — decode + pairing check | To build |
| 2 | Wrong bitfield length (`⌈n/8⌉ ± 1` bytes) | bitfield array sized wrong | `MalformedSigners` | **Cheapest** — length compare, before any BLS math | **DONE** — see [`README.md`](./README.md) |
| 3 | Wrong-context aggregate | a *valid* aggregate signed over a **different** RB hash | `InvalidSignature` | **Dearest** — full aggregate-pubkey reconstruction, then pairing against the wrong message | **DONE** — see [`README.md`](./README.md) |
| 4 | Non-member bits | bits set past `committee_size` (`idx ≥ n`) | bits dropped by the `idx < n` guard → residual weight decides: `InsufficientWeight` (if the drop leaves sub-quorum) or `InvalidSignature` | Medium — decode + guard walk, aggregate check only if weight clears | **DONE** — see [`README.md`](./README.md) |
| 5 | Cert present, no certifiable EB (`certified_eb = false`) | header/body cert flag disagreement; cert with nothing to certify | `InvalidBlock` (header claims a cert it should not carry / carries one with no certifiable EB) | Cheap-to-medium — header/body consistency, before deep cert math | To build — **T104 boundary, see note** |

**Legend.** "Cheapest/dearest" = where in `verifyLeiosCert` the reject fires and
how much work precedes it (length compare < bitfield decode < aggregate-pubkey
reconstruction < pairing). `idx < n` guard = the ledger's `when (idx < n)` filter
that drops signer indices at or past the committee bound (mirrored in `cert.rs`
`build_bitfield`/`bitfield_ids`). `certified_eb` = the RB header flag asserting
the block carries a certificate.

**Variant 5 / T104 overlap (flag explicitly).** Variant 5 sits on the boundary
with **T104** ("Malformed cert / inline RB — cert-XOR-inline violation"; owner
cet, bead `zts`, see [`../docs/threat-coverage.md`](../docs/threat-coverage.md)
line 80). T104 covers forging an RB that violates cert-XOR-inline (a cert
*together with* inline txs, or a cert with `certified_eb = false`). Where T14
variant 5 and T104 overlap, this plan defers the cert-XOR-inline case to T104 and
scopes T14 variant 5 to the narrower "cert present, `certified_eb` flag says
otherwise" cost measurement, cross-referencing T104 so the two are not
double-counted.

## Measurement plan (T14's distinct contribution)

T15 measured **adoption** (safety: 0/N). T14 measures **cost** and **reach**,
because a bad-cert RB that is always rejected can still impose work and still
diffuse before it dies. Metrics per variant, on the honest node:

1. **Verification cost per bad-cert RB** — CPU and wall-time for the
   fetch → VolatileDB add → ledger-validate → discard round-trip. The reject
   *ground* (from the honest node's `InvalidBlock` trace) tells us which cost
   bucket the variant landed in; the wall-time quantifies the asymmetry between
   buckets.
2. **Rejection locality (reach)** — does the *first* honest hop drop the block,
   or does it diffuse to a second hop before rejection? A block rejected at the
   first hop costs one node; one that diffuses first multiplies the cost by fan-out.
3. **Throughput under a stream** — wire the forge branch to the existing
   **fetch-flood** harness (`behaviours/attacks/fetch-flood.bt`,
   `net-rs/net-cluster/behaviours/fetch-flood.toml`) so net-rs emits bad-cert RBs
   at a sustained rate, and measure whether honest **block adoption** (the smoke
   settlement metric) measurably slows versus a clean baseline.

**Headline finding T14 is after.** The **asymmetry**: cheapest-to-reject variant
(#2, wrong length → `MalformedSigners`, a length compare) versus dearest (#3,
wrong-context → full aggregate-pubkey reconstruction) = how much asymmetric work
a producer can force an honest node to do per forged block. That ratio, times the
sustainable forge rate under the fetch-flood harness, is the T14 nuisance ceiling.

**Instrumentation source (to confirm).** Honest-node cost is read from
`cardano-node`'s own tracing / `InvalidBlock` events plus wall-clock deltas; net-rs
telemetry (`cluster-events.jsonl`, `:9100`, `net-rs/logs/node-*.log`) supplies the
forge-side ground truth (which RB carried which forged variant). Whether the
upstream node exposes per-block validation timing at sufficient granularity is
**to confirm** before committing to metric #1's precision.

## Method / harness constraints (carried over from T15)

These are **mandates**, not choices, and are verified against the repo:

- **Fresh devnet + fresh magazine, every run; 3× for any consensus-affecting
  result.** Enforced in our internal `proto-devnet/scripts/smoke.sh`:
  `SMOKE_FRESH=1` is the default (line 90; every run `make reset`s to clean
  genesis), a timestamped magazine is built per run (line 82), and the script
  **refuses a non-fresh run** unless `SMOKE_REUSE=1` is passed deliberately
  (lines 184–224). Never
  claim a PASS/FAIL from a reused devnet/magazine or a single unlucky run.
- **The `pmr` slot-monotonicity fix is a prerequisite.** net-rs must not forge at
  a slot ≤ its adopted parent, or the honest node rejects at the **header
  envelope** (`UnexpectedSlotNo` / same-slot) *before* `verifyLeiosCert` is ever
  reached — the forged certificate would never be judged. The T15 report names
  this fix as branch `fix/netrs-slot-monotonicity`. **Verified: it has merged to
  `main`** as commit `f325bac` ("net-rs: fix same-slot forge
  (slot-monotonicity) + smoke gate [pmr]", PR #101) and is an ancestor of the
  current `HEAD`; the `fix/netrs-slot-monotonicity` branch still exists but is
  now redundant. No separate checkout is needed.
- **Honest nodes are unmodified upstream `cardano-node`.** The devnet runs the
  **w38** Leios node binary — verified from our internal
  `proto-devnet/scripts/env.sh`, which
  points `CARDANO_BIN` at `cardano-node-leios-w38/bin`. The exact patch version
  the T15 report cites (**11.1.0.164**) is **to confirm** — it was not printed in
  `env.sh` (only the `w38` install path was), so it should be read from the
  running binary (`cardano-node --version`) before a live run.
- **Stake split.** net-rs runs as a ~20%-stake pool against three honest
  80%-stake Haskell pools. Do **not** alter genesis or staking to change this —
  it is a devnet guardrail.

## Prospective results (template — nothing run yet)

To be filled per variant, 3× on a fresh devnet + fresh magazine each. Column
shape mirrors T15's results table.

| Variant | forged RBs shipped | honest reject ground | adopted | median validate cost | reach (hops) |
|---------|--------------------|-----------------------|---------|----------------------|--------------|
| #1 garbage aggregate | — | (expect `InvalidSignature`) | (expect 0) | — | — |
| #2 wrong length | — | (expect `MalformedSigners`) | (expect 0) | — | — |
| #3 wrong-context | — | (expect `InvalidSignature`) | (expect 0) | — | — |
| #4 non-member bits | — | (expect `InsufficientWeight` / `InvalidSignature`) | (expect 0) | — | — |
| #5 no certifiable EB | — | (expect `InvalidBlock`) | (expect 0) | — | — |

**Legend.** *forged RBs shipped* = distinct net-rs blocks published carrying the
forged variant (counted from honest-node rejections, not adversary logs, per
T15's method); *honest reject ground* = the `verifyLeiosCert` / `InvalidBlock`
verdict; *adopted* = forged-cert blocks that entered any honest chain (safety
cross-check — expected 0, since T15 already proved the width lie sound);
*median validate cost* = wall-time for fetch → VolatileDB → validate → discard;
*reach* = honest hops the block reached before rejection.

## Conclusion (expected shape — to be confirmed by runs)

T14 is expected to confirm that **every** invalid-certificate variant is
rejected (safety intact, consistent with T15), while quantifying the **cost
asymmetry** the adversary can exploit: cheap rejects (`MalformedSigners`,
length-only) versus expensive ones (`InvalidSignature` via full aggregate-pubkey
reconstruction), and whether a sustained stream of the expensive variant slows
honest adoption. The distinct T14 deliverable is that cost/reach profile, not a
new safety verdict.

## Open items to confirm before building

- Exact honest-node binary patch version (T15 cites w38 / 11.1.0.164; `env.sh`
  confirms w38 only — read `cardano-node --version` on the devnet).
- Whether upstream `cardano-node` tracing exposes per-block validation timing at
  the granularity metric #1 needs.
- Precise T14 / T104 boundary for variant 5 (cert-XOR-inline is T104's; confirm
  with cet / bead `zts`).

## References

- Completed baseline: [`../t15/single-bls-cert.md`](../t15/single-bls-cert.md)
  (T15 safety audit — verdict SOUND).
- Threat coverage: [`../docs/threat-coverage.md`](../docs/threat-coverage.md)
  (T14 row line 56; T104 overlap line 80).
- Adversary actuation (internal `net-rs` implementation):
  `net-rs/net-node/src/consensus/leios/cert.rs`,
  `net-rs/net-node/src/consensus/mod.rs`,
  `net-rs/net-node/src/main.rs` (forge branch ~1424–1494),
  `behaviours/attacks/single-bls-cert.bt`.
- Engine (this repo):
  [`../../../shared-rs/consensus/src/behaviour/registry.rs`](../../../shared-rs/consensus/src/behaviour/registry.rs)
  (`SingleBlsCert` leaf, `ControlSignal.praos.forge_single_bls_cert`).
- Harness (internal): `proto-devnet/scripts/smoke.sh`
  (fresh-devnet gate), `proto-devnet/scripts/env.sh`
  (w38 binary), `behaviours/attacks/fetch-flood.bt` /
  `net-rs/net-cluster/behaviours/fetch-flood.toml` (stream harness).
- Prerequisite fix: `f325bac` (PR #101, slot-monotonicity, on `main`).
