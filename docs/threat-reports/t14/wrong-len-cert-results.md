# T14 — wrong-len-cert: invalid-certificate-in-RB audit (variant #2)

Companion to [`test-plan.md`](./test-plan.md) (the prospective plan) and the
completed T15 baseline [`../t15/single-bls-cert.md`](../t15/single-bls-cert.md).
This is the **results** writeup for T14 variant #2, the wrong-length signer
bitfield.

**Question.** When an adversarial producer ships a ranking block (RB) carrying a
certificate whose `signers` bitfield is the wrong length, does the honest Haskell
Leios ledger reject the block at its length check (`MalformedSigners`) before any
BLS aggregate math, and does it adopt none of them?

**Verdict — rejection is SOUND.** Across three fresh devnets the honest ledger
rejected **every** forged-cert block with `LeiosInvalidCertificate … MalformedSigners`
and adopted **none**. No length-check gap was found.

## The attack

The on-chain `leios_certificate` is the CBOR 2-tuple
`[signers : bytes, aggregated_signature : bytes .size 48]`. The ledger's
`verifyLeiosCert` accepts the `signers` bitfield only at its mandated length,
`⌈committee_size / 8⌉` bytes; any other length is `MalformedSigners` and the
carrying RB is an `InvalidBlock`.

The `wrong-len-cert` behaviour makes net-rs forge that tuple with a bitfield that
is **one byte too long** (`⌈committee_size / 8⌉ + 1`):

- `signers` = the all-committee claim padded with one trailing `0x00`. On the
  4-pool devnet the valid width is one byte, so the forgery ships **two**:
  `[0xf0, 0x00]`.
- `aggregated_signature` = a genuine, well-formed single-key G1 aggregate. The
  only defect is the length; the named signer set and the signature are otherwise
  honest, so the length check is the sole thing under test.

`+1` (not `-1`) is used deliberately: `⌈n/8⌉ + 1` is unambiguously wrong for every
committee size, whereas `⌈n/8⌉ - 1` is exactly the valid width of a slightly
smaller committee and underflows to zero bytes at the smallest sizes. The producer
skips its usual pre-publish self-verify drop (which exists to keep an *accidental*
bad cert off its own chain) so the forged block actually reaches the honest ledger.

Actuation is entirely in the adversary node; the honest nodes are unmodified
upstream `cardano-node` (w38).

## Method

net-rs runs as a **20%-stake** pool against three honest 80% Haskell pools on the
proto-devnet, armed with `NETRS_BEHAVIOUR=wrong-len-cert`. Run **3×, each on a
fresh devnet + fresh magazine** (`SMOKE_FRESH=1 SMOKE_COUNT=2400
SMOKE_TIMEOUT=3600 make smoke`), per the smoke-integrity mandate. The `pmr`
slot-monotonicity fix (on `main`) is a prerequisite so net-rs's CertRBs clear the
header envelope and actually reach `verifyLeiosCert`; all three runs held **0 slot
collisions**.

The forged certificate rides only when net-rs forges the CertRB that certifies a
certifiable EB, so the count of forged certs that reach the honest ledger is small
even though net-rs logs many forge attempts. Distinct forged-cert blocks are
counted authoritatively from the honest nodes' `InvalidBlock` rejections, not from
the adversary's logs (the raw honest log-line count is higher because each
rejection is echoed under a second tracer namespace).

## Results

| Run | net-rs forge warns | distinct forged certs reaching honest ledger | honest verdict (×3 nodes) | adopted | slot collisions | tip-miss |
|-----|--------------------|----------------------------------------------|---------------------------|---------|-----------------|----------|
| 1   | 204 | 3 (slots 1115, 2004, 2961)            | `MalformedSigners`        | 0 | 0 | 25% |
| 2   | 329 | 2 (slots 1495, 2432)                  | `MalformedSigners`        | 0 | 0 | 16% |
| 3   | 223 | 4 (slots 848, 1948, 2135, 2204)       | `MalformedSigners`        | 0 | 0 | 30% |
| **Σ** | — | **9**                                | **all rejected**          | **0** | **0** | — |

Legend: *net-rs forge warns* = `AUDIT wrong-len-cert` log lines on the adversary
(blocks on which it embedded a forged cert, most on its own orphaned forks);
*distinct forged certs reaching honest ledger* = distinct net-rs blocks the honest
nodes actually fetched and validated, counted from their `InvalidBlock` rejections;
*honest verdict* = each honest node's ledger outcome; *adopted* = forged-cert blocks
entering any honest node's chain; *slot collisions* = net-rs blocks forged at a slot
≤ their adopted parent's (the `pmr` gate); *tip-miss* = `chainTipDoesNotAnnounce`
rate, the honest-consensus-health metric (see below).

Every forged cert in all three runs carried the `[0xf0, 0x00]` wrong-length
bitfield and was rejected `MalformedSigners`. Zero unrelated cert rejections.

### Representative rejection (all three honest nodes, identical)

```
ChainDB.AddBlockEvent.AddBlockValidation.InvalidBlock
error = ExtValidationErrorLeios
  (LeiosInvalidCertificate
     (LeiosCert { leiosCertSigners   = BitField { bitFieldBytes = [0xf0, 0x00] }  -- one byte too long (forged)
                , leiosCertSignature = SigBLS12381 "4c0fabb6…" })
     (1102, 733a6f67e4…)  8022a0ed0d…  MalformedSigners)                          -- rejected at the length check
  kind = InvalidBlock
```

## On the "SMOKE FAIL" banner

`make smoke`'s PASS/FAIL gate measures **honest** consensus health (EB-only txs
settling, tip-adoption rate). An adversarial certificate behaviour is expected to
trip it, exactly as the T15 baseline did: with net-rs shipping invalid certs, its
own cert-carrying blocks are rejected and orphaned, raising its
`chainTipDoesNotAnnounce` rate above the 15% honest gate (25% / 16% / 30% here).
That FAIL is **not** the T14 verdict. The T14 audit oracle — forged certs rejected
at the length check, none adopted, no slot collisions — passed on all three runs.
The honest pools kept certifying their own EBs throughout (14–18 certs, 519–966
txs settled per run), so the adversary degraded only its own contribution.

### Reach

net-rs embedded a forged cert on 200–330 blocks per run, but only 2–4 per run
reached honest-ledger validation; the rest rode net-rs's own orphaned forks the
honest nodes never fetched. A wrong-length cert therefore imposes honest
validation cost on only the handful of net-rs blocks that are actually adopted-then-
fetched, and each of those is rejected at the cheapest possible stage (a length
comparison, before BLS work). This is the low end of the T14 cost spectrum the
plan describes.

## Conclusion

The Haskell Leios `verifyLeiosCert` enforces the `⌈committee_size / 8⌉`-byte
bitfield length and rejects a wrong-length certificate as `MalformedSigners`,
failing the whole RB, before any signature verification. Confirmed 3× on fresh
devnets: 9 forged certs shipped to the honest ledger, all rejected, none adopted,
zero slot collisions. No exploitable length-check gap.

## Status

Implementation is unit-tested (`cargo test -p net-node`, engine
`shared-rs/consensus`) and confirmed on the devnet 3×. The remaining T14
variants are covered in [`README.md`](./README.md): wrong-context-cert and
non-member-cert are implemented and confirmed; the garbage-aggregate variant
is described in [`test-plan.md`](./test-plan.md) and not implemented, and
cert-with-no-certifiable-EB is out of scope here (tracked under T104).

## References

- Plan: [`test-plan.md`](./test-plan.md). Baseline: [`../t15/single-bls-cert.md`](../t15/single-bls-cert.md).
- Adversary actuation: `net-rs/net-node/src/consensus/leios/cert.rs`
  (`build_wrong_len_cert`), `net-rs/net-node/src/consensus/mod.rs`
  (`wrong_len_certificate_for_parent`), `net-rs/net-node/src/main.rs` (forge
  branch, `control.praos.forge_wrong_len_cert`), behaviour
  `behaviours/attacks/wrong-len-cert.bt`.
- Engine (public `leios-tools`, branch `feat/t14-wrong-len-cert`):
  `shared-rs/consensus/src/behaviour/…` — `WrongLenCert` action leaf +
  `forge_wrong_len_cert` control flag.
- Harness: `proto-devnet/scripts/smoke.sh`, `proto-devnet/scripts/slot-monotonicity.sh`.
