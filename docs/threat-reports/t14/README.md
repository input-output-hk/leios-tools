# T14 — Include invalid certificate in RB: combined Red Team report

Threat **T14** (Ouroboros Leios threat model, *Inaction and Nuisance*): a block
producer forges a valid Praos ranking block (RB) that carries an **invalid Leios
certificate**, imposing verification work on honest nodes. This report covers the
Red Team's four adversarial-certificate variants, each implemented on net-rs and
confirmed against unmodified honest Haskell nodes (`cardano-node` w38) on the
proto-devnet.

Companion docs: the prospective plan [`test-plan.md`](./test-plan.md); the
per-variant results in [`wrong-len-cert-results.md`](./wrong-len-cert-results.md)
and the T15 baseline [`../t15/single-bls-cert.md`](../t15/single-bls-cert.md).

## Result in one line

Every variant is soundly rejected. Across all runs the honest ledger rejected
**every** forged-certificate block and adopted **none**, with **zero** net-rs slot
collisions. No certificate-verification gap was found. The four variants exercise
**three distinct rejection verdicts** at three different stages of `verifyLeiosCert`,
which maps the cost of rejecting a bad certificate from cheapest to dearest.

## The four variants

| # | Variant | Forged defect | Bitfield (4-seat devnet) | Rejection stage | Verdict |
|---|---------|---------------|--------------------------|-----------------|---------|
| 1 | single-bls-cert | all-committee bitfield, one-key aggregate | `0xf0` (correct len) | aggregate vs named signer set | `InvalidSignature` |
| 2 | wrong-len-cert | bitfield one byte too long | `[0xf0, 0x00]` (len +1) | length check (earliest) | `MalformedSigners` |
| 3 | wrong-context-cert | valid aggregate over the wrong RB hash | `0xf0` (correct len) | aggregate pairing (latest) | `InvalidSignature` |
| 4 | non-member-cert | only out-of-range signer bits set | `0x0f` (correct len) | committee-weight gate | `InsufficientWeight` |

Legend: *bitfield* = the cert's `signers` byte array on the 4-pool devnet committee
(valid length ⌈4/8⌉ = 1 byte); *rejection stage* = where in the ledger's
`verifyLeiosCert` the block is rejected; *verdict* = the `LeiosInvalidCertificate`
reason recorded by every honest node. RB = ranking block.

Variant 1 is the T15 audit (the user-identified starting point: a single BLS
signature under an all-ones bitmask); variants 2 to 4 are new T14 work.

## Cost asymmetry (T14's distinct finding)

T14 is a throughput/nuisance threat, so the question is not only "is it rejected"
(yes, always) but "how much work does rejecting it cost." The variants rank the
honest node's rejection cost:

- **Cheapest — `MalformedSigners` (wrong-len).** Rejected at a length comparison,
  before any BLS math. The honest node spends almost nothing.
- **Middle — `InsufficientWeight` (non-member).** Passes the length check; the
  ledger's `idx < n` guard drops every out-of-range bit, leaving zero committee
  weight, and rejects before verifying the aggregate.
- **Dearest — `InvalidSignature` (wrong-context, single-bls).** Passes the length
  and weight gates; the ledger reconstructs the named signers' aggregate public key
  and runs the pairing check before rejecting. This is the most expensive path a
  single forged certificate can force.

A producer therefore cannot make an honest node do *more* than one aggregate
verification per forged block, and most cheaper forgeries are discarded earlier.

## Method

net-rs runs as a 20%-stake pool against three honest 80% Haskell pools, armed via
`NETRS_BEHAVIOUR=<variant>`. Each variant was run **3×, each on a fresh devnet +
fresh magazine** (`SMOKE_FRESH=1 SMOKE_COUNT=2400 SMOKE_TIMEOUT=3600 make smoke`),
per the smoke-integrity mandate. The `pmr` slot-monotonicity fix (on `main`) is a
prerequisite so net-rs's CertRBs clear the header envelope and reach
`verifyLeiosCert`; **0 slot collisions on every run**. A forged certificate rides
only when net-rs forges the CertRB that certifies a certifiable EB, so the count of
forged certs reaching the honest ledger is small; distinct forged-cert blocks are
counted from the honest nodes' `InvalidBlock` rejections, not the adversary's logs.

## Results (3× fresh devnet per variant)

| Variant | Run 1 | Run 2 | Run 3 | Σ forged certs at honest ledger | verdict (all honest nodes) | adopted | slot collisions |
|---------|-------|-------|-------|---------------------------------|----------------------------|---------|-----------------|
| 1 single-bls-cert | 2 | 3 | 2 | 7 | `InvalidSignature` | 0 | 0 |
| 2 wrong-len-cert | 3 | 2 | 4 | 9 | `MalformedSigners` | 0 | 0 |
| 3 wrong-context-cert | 3 | 1 | 3 | 7 | `InvalidSignature` | 0 | 0 |
| 4 non-member-cert | 2 | 2 | 0\* | 4 | `InsufficientWeight` | 0 | 0 |

Legend: each Run cell = distinct net-rs blocks carrying a forged certificate that
the honest nodes fetched and validated that run; *Σ forged certs at honest ledger* =
total across the three runs; *adopted* = forged-cert blocks entering any honest
node's chain; *slot collisions* = net-rs blocks forged at a slot ≤ their adopted
parent's (the `pmr` gate). Variant 1's figures are from the T15 audit.

\* Non-member run 3 is a **null run**: net-rs armed and forged (160 forged-cert
blocks logged) but a high net-rs tip-miss (26%) that run orphaned its CertRBs before
any honest node fetched them, so none reached `verifyLeiosCert`. It adds no positive
rejection but confirms 0 adopted / 0 collisions, and does not contradict runs 1 to 2.
The `InsufficientWeight` verdict is confirmed twice and contradicted zero times, and
is proved deterministically by the unit tests.

### Representative verdicts

```
wrong-len:     LeiosInvalidCertificate (… bitFieldBytes = [0xf0, 0x00] …) MalformedSigners
wrong-context: LeiosInvalidCertificate (… bitFieldBytes = [0xf0]        …) InvalidSignature
non-member:    LeiosInvalidCertificate (… bitFieldBytes = [0x0f]        …) InsufficientWeight
```

## Caveats

- **The `SMOKE FAIL` banner is expected, not the audit verdict.** `make smoke`
  grades *honest* consensus health (tip-adoption, settlement). An adversarial
  certificate behaviour trips it, because net-rs's own cert-carrying blocks are
  rejected and orphaned, lowering its RB-adoption and raising its tip-miss below
  the honest gates. The honest pools kept certifying their own EBs throughout. The
  T14 oracle (rejection + non-adoption + no slot collisions) passed on every run.
- **wrong-context and single-bls are over-determined on a multi-seat committee.**
  A single adversary holds only its own BLS key, so it ships a one-key aggregate
  under an all-committee bitfield. On the 4-seat devnet the `InvalidSignature`
  verdict is caused by both the wrong message *and* the one-key-vs-full-committee
  signer-set mismatch. The pure context-binding property is isolated only where the
  named signer set equals the actual signer (the N=1 committee unit test); a
  single-adversary full-committee aggregate over the wrong hash is not constructible,
  because honest voters only ever sign the real hash. This is inherent to the attack,
  not a wiring defect.

## Conclusion

Honest Leios certificate verification is sound against all four invalid-certificate
variants. It rejects at the earliest sufficient stage (length, then committee
weight, then aggregate signature), fails the whole RB, and adopts none. No variant
forced more than one aggregate verification per forged block, and no variant caused
a slot collision or a chain-adoption safety issue.

## Status and remaining work

All four variants are implemented, unit-tested (`cargo test -p net-node` 225 pass;
engine `shared-rs/consensus` 456 pass) and confirmed on the devnet 3× each. Two
cases from [`test-plan.md`](./test-plan.md) are **not** covered here:

- **Garbage / zero aggregate** (test-plan variant #1; not this report's #1):
  the 48-byte aggregate replaced with zero or random bytes over an honest
  bitfield. None of the four variants above corrupts the aggregate bytes, so
  the bad-G1-point reject path is untested. Still to build.
- **Cert with no certifiable EB** (cert-XOR-inline): **out of scope here**. It
  overlaps threat **T104** and is tracked there.

## References

- Code (net-rs, branch `feat/t14`): `net-rs/net-node/src/consensus/leios/cert.rs`
  (`build_single_bls_cert`, `build_wrong_len_cert`, `build_wrong_context_cert`,
  `build_non_member_cert`), `.../consensus/mod.rs` (the `*_certificate_for_parent`
  selectors), `net-rs/net-node/src/main.rs` (the forge branch and self-verify-drop
  bypass), `behaviours/attacks/{single-bls,wrong-len,wrong-context,non-member}-cert.bt`.
- Engine (public `leios-tools`, `main` — PR #103):
  `shared-rs/consensus/src/behaviour/…` — the four action leaves and
  `forge_*` control flags.
- Harness: `proto-devnet/scripts/smoke.sh`, `proto-devnet/scripts/slot-monotonicity.sh`.
