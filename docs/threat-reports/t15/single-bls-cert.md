# T15 — single-bls-cert: BLS aggregate-verification audit

**Question.** Does the honest Haskell Leios ledger actually verify the *aggregate*
BLS signature in an EB certificate, or does it trust the certificate's signer
bitfield (the claimed weight) without binding the aggregate to the named signers?

**Verdict — verification is SOUND.** The ledger reconstructs the aggregate public
key of the *named* signers and checks the aggregate against it. A certificate whose
bitfield claims the whole committee but whose aggregate is only one key is rejected
with `LeiosInvalidCertificate … InvalidSignature`. No certificate-forgery soundness
break was found.

## The attack

The on-chain `leios_certificate` is the CBOR 2-tuple
`[signers : bytes, aggregated_signature : bytes .size 48]`, where `signers` is an
MSB-first bitfield over the committee (⌈n/8⌉ bytes) and `aggregated_signature` is
the 48-byte G1 aggregate of the named members' vote signatures over the announcing
RB's hash.

The `single-bls-cert` behaviour makes the adversary (net-rs) forge that tuple on the
CertRB it produces:

- `signers` = **all committee seats set** (on the 4-pool devnet, one byte `0xf0`).
- `aggregated_signature` = **only net-rs's own** BLS vote signature over the
  announcing RB hash (a valid single-key signature, run through the real aggregator
  so the wire form is byte-identical to an honest 1-signer cert).

The all-committee bitfield clears the ledger's weight gate (`InsufficientWeight`),
leaving exactly one thing between the block and adoption: whether `verifyLeiosCert`
binds the aggregate to the named signer set.

- **Reject (`InvalidSignature`)** ⇒ the aggregate is checked against the named keys:
  verification holds.
- **Adopt** ⇒ the bitfield's weight was trusted without checking the aggregate: a
  one-key adversary could forge full-quorum certificates — a soundness break.

Actuation is entirely in the adversary node; the honest nodes are unmodified
upstream `cardano-node` (w38, 11.1.0.164).

## Method

net-rs runs as a **20%-stake** pool against three honest 80% Haskell pools on the
proto-devnet, armed with the `single-bls-cert` behaviour. The forged certificate can
only reach `verifyLeiosCert` when net-rs actually forges the block that certifies a
certifiable EB (the immediate child of the announcer, ≥ 12 slots later, once the EB
is `CertEligible`). Run **3× on a fresh devnet + fresh magazine** each
(`NETRS_BEHAVIOUR=single-bls-cert SMOKE_FRESH=1 make smoke`, `SMOKE_COUNT=2400`,
`SMOKE_TIMEOUT=3600`).

### Prerequisite: the pmr slot-monotonicity fix

The audit first surfaced a real net-rs consensus bug (`bd pmr`): net-rs could forge a
block at the **same slot** as its just-adopted parent, which the honest nodes reject
at the header envelope (`UnexpectedSlotNo`) *before* the body — and thus
`verifyLeiosCert` — is ever reached, orphaning the block and churning peers. With the
fix (refuse to forge unless `candidate_slot > adopted_tip_slot`; branch
`fix/netrs-slot-monotonicity`), net-rs's valid CertRBs reach ledger validation and
the forged certificate is actually judged. The fix is a prerequisite for this audit,
and confirmed **0 slot collisions on all three fresh runs**.

## Results

| Run | forged certs shipped | honest verdict | adopted | slot collisions | tip-miss |
|-----|----------------------|----------------|---------|-----------------|----------|
| 1   | 2                    | `InvalidSignature` (×3 nodes) | 0 | 0 | 16% |
| 2   | 3                    | `InvalidSignature` (×3 nodes) | 0 | 0 | 14% |
| 3   | 2                    | `InvalidSignature` (×3 nodes) | 0 | 0 |  8% |
| **Σ** | **7**              | **all rejected** | **0** | **0** | — |

Legend: *forged certs shipped* = distinct net-rs blocks published carrying a forged
`0xf0` certificate (counted authoritatively from honest-node rejections, not the
adversary's logs); *honest verdict* = each honest node's ledger outcome for that
block; *adopted* = forged-cert blocks that entered any honest node's chain; *slot
collisions* = net-rs blocks forged at a slot ≤ their adopted parent's (bd pmr gate);
*tip-miss* = `chainTipDoesNotAnnounce` rate (an unrelated adoption metric).

Across three fresh devnets net-rs shipped **7 distinct forged certificates**; the
honest ledger rejected **every one** and adopted **none**. Every certificate
rejection in all three runs was our `0xf0` forgery — **zero** unrelated cert
rejections (no noise).

### Representative rejection (all three honest nodes, identical)

```
ChainDB.AddBlockEvent.AddBlockValidation.InvalidBlock
error = ExtValidationErrorLeios
  (LeiosInvalidCertificate
     (LeiosCert { leiosCertSigners   = BitField { bitFieldBytes = [0xf0] }   -- all-committee claim (forged)
                , leiosCertSignature = SigBLS12381 "7bd8a214…" })            -- our one-key aggregate
     (492, 440f70474e…)  8160a34b39c3fc0b…  InvalidSignature)                -- the ledger's verdict
  kind = InvalidBlock
```

The honest node fetched the block, added it to its VolatileDB, then rejected it at
ledger validation: it reconstructed the aggregate public key of the four named seats
and found the one-key aggregate did not verify against it.

## Conclusion

The Haskell Leios `verifyLeiosCert` correctly binds the aggregate BLS signature to
the committee members named by the `signers` bitfield. It does not accept a
certificate on the strength of the bitfield's claimed weight alone. The
`single-bls-cert` forgery is soundly rejected — no exploitable gap in EB-certificate
aggregate verification.

## References

- Beads: `leios_adversarial_tools-bf2` (this audit), `leios_adversarial_tools-pmr`
  (the slot-monotonicity prerequisite bug/fix).
- Adversary actuation: `net-rs/net-node/src/consensus/leios/cert.rs`
  (`build_single_bls_cert`), `net-rs/net-node/src/consensus/mod.rs`
  (`single_bls_certificate_for_parent`), `net-rs/net-node/src/main.rs`
  (forge branch), behaviour `behaviours/attacks/single-bls-cert.bt`.
- Engine (public `leios-tools`): `shared-rs/consensus/src/behaviour/…`
  (`ControlSignal.praos.forge_single_bls_cert`, the `SingleBlsCert` action leaf).
- Harness gate: `proto-devnet/scripts/slot-monotonicity.sh`, `proto-devnet/scripts/smoke.sh`.
