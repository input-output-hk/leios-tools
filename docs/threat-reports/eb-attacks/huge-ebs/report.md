# Huge (over-wire-limit) EBs → rejected at the LeiosFetch size gate (defended)

**Date:** 2026-09-03
**Environment:** proto-devnet (local), 3 Haskell BP nodes + net-rs (Piranha) 4th pool
**Attack:** `phantom-tx-eb` with `n_txs = 100000` (~3.8 MB EB body — above the wire limit)
**Target:** cardano-node **w34** (`prototype-2026w34`; `11.1.0.164`, git `3e1bec02`, `ouroboros-consensus-3.0.1.0`)
**Verdict:** **defended.** An EB whose body exceeds the LeiosFetch message size limit
is rejected with `ExceededSizeLimit` *before* it is decoded or stored. No crash, no
OOM; the bounded-receive guard works as intended.

> Companion to the item-count finding in `../oversized-eb/report.md`. Together they
> show the two size gates are complementary: the **byte** gate holds; the **item-count**
> gate (13,888-entry vector) does not. This report is the "byte" half.

---

## 1. Summary

The question was whether a *truly huge* EB — large enough in bytes to exceed the
node's receive/message buffers — is an attack. It is not, on w34: net-rs forged a
~3.8 MB phantom EB (`n_txs = 100000`) and offered it; when net-rs served it over
LeiosFetch, the receiving node's codec detected the message exceeded its size limit
and returned `ExceededSizeLimit`, tearing down the connection without decoding,
storing, or over-allocating. The chain kept advancing and no node crashed.

This result also **delimits** the item-count bug from the companion report: the
13,888-vector overflow is reachable only by EBs that are *small in bytes but large in
count*, because anything large enough in bytes to matter is rejected at the byte gate
first.

## 2. Environment

| Component | Value |
|---|---|
| cardano-node | **w34** (`prototype-2026w34`); `11.1.0.164`, ghc-9.6, git `3e1bec02` |
| consensus | `ouroboros-consensus-3.0.1.0` |
| Committee | node1/2/3 (`:4001`/`:4002`/`:4003`), ~24T each |
| Adversary | net-rs (`:4010`), 4th pool, ~20% stake |

## 3. The test

- `phantom-tx-eb` builds a manifest of `n_txs` random 32-byte hashes (~38 bytes/entry
  encoded). `n_txs = 100000` ⇒ **~3.8 MB** EB body — comfortably above the assessed
  ~2.5 MB generic wire limit.
- net-rs's clamp allows it: `phantom max_txs = eb_body_max_bytes / 32 = 512,000`, so
  100,000 is not clamped, and net-rs's own `eb_body_max` (16 MB) permits building it.
- Armed via `POST /api/attack` with the `n_txs` parameter override at
  **2026-09-03T17:39:37Z**.

## 4. Evidence

net-rs forged the oversized EBs (blocks adopted post-arm at slots 92647, 92682) and
served them. Each serve produced, on the fetching node:

```
Net.InboundGovernor.Remote.MuxErrored / Net.ConnectionManager.Remote.ConnectionHandler.Error
  reason: ExceededSizeLimit (LeiosFetch LeiosPoint LeiosEb LeiosTx) ServerHasAgency (SingBlock)
```

- **`ExceededSizeLimit`** on the **LeiosFetch** protocol, `ServerHasAgency (SingBlock)`
  — the fetch client rejected net-rs's oversized block *response* at the codec size
  check.
- **Clean discriminator:** since arm, `ExceededSizeLimit` rejections = 2 per node,
  `index out of bounds (13888)` = **0**. The size gate fires **before** the manifest is
  decoded, so the item-count vector overflow is never reached.
- **No crash, no OOM.** All three nodes stayed up; the chain advanced across the test
  (block ~4274 → ~4286). Connection teardown/re-establish only.

## 5. The two-gate picture

| Gate | Enforced? | Effect |
|---|---|---|
| **Byte size** (LeiosFetch message limit, ~2.5 MB) | **Yes** | EB > limit → `ExceededSizeLimit`, rejected pre-decode |
| **Item count** (per-EB 13,888 vector) | **No** | EB ≤ byte limit with > 13,888 items → vector overflow (see `../oversized-eb/`) |

**Legend:** *byte size* = total encoded EB-body message length; *item count* = number of
manifest entries; *pre-decode* = rejected before the CBOR body is parsed or stored.

The exploitable item-count window is therefore **~528 KB (13,889 items) up to ~2.5 MB
(~65,000 items)** — small enough to pass the byte gate, large enough in count to
overflow the vector. Above ~2.5 MB the byte gate catches it first (this report);
below 13,889 items nothing overflows.

## 6. Reproduction

```bash
curl -X POST http://127.0.0.1:9100/api/attack -H 'content-type: application/json' \
 -d '{"id":"phantom-tx-eb","selection":{"kind":"all"},
      "parameters":[{"toml_path":"behaviours.phantom-tx-eb.spec.n_txs","value":100000}]}'

grep -E "ExceededSizeLimit|index out of bounds \(13888" ~/leios-proto-devnet/node{1,2,3}/node.log
curl -X POST http://127.0.0.1:9100/api/attack/stop
```

Expected: `ExceededSizeLimit (LeiosFetch ...)` and **zero** `index out of bounds
(13888)` — the byte gate rejects the oversized EB before the vector path.

## 7. Conclusion & the remaining probe

- **A genuinely-huge-byte EB is not a viable attack on w34** — the LeiosFetch size
  limit is enforced on received bytes, so the buffer/allocation gate holds.
- The real gap is the **item-count** vector overflow (companion report), reachable
  only in the sub-limit byte window.
- **Still open — declared-size pre-allocation (`Hollow`).** The byte gate bounds
  *actual* bytes received; it does not tell us whether a large *declared* `eb_size`
  (announcement/offer) drives a pre-allocation before the (empty) body is fetched.
  That is a separate, near-zero-cost vector, probed by wiring `FakeEbKind::Hollow`
  (declare huge, serve nothing). Spec + sweep tracked in this directory's Hollow work.
