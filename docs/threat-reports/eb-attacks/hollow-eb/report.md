# Hollow EB (empty body, false declared size) → declared-size pre-allocation probe

**Date:** 2026-09-03
**Environment:** proto-devnet (local), 3 Haskell BP nodes + net-rs (Piranha) 4th pool
**Attack:** `hollow-eb` with `declared_bytes` swept (newly wired — see §3)
**Target:** cardano-node **w34** (`prototype-2026w34`; `11.1.0.164`, git `3e1bec02`, `ouroboros-consensus-3.0.1.0`)
**Verdict:** **declared-size pre-allocation is DEFENDED.** A large declared `eb_size`
on an empty body does **not** cause the receiver to pre-allocate — RSS stays bounded
(~160 MB, never toward the declared value). A declared size **above the wire limit**
does trigger an unhandled size-mismatch `error` → connection teardown (the "EB Size
Lie" class), but a declared size **below the wire limit is accepted with no teardown
and no growth**.

> Companion to `../oversized-eb/report.md` (item-count vector overflow) and
> `../huge-ebs/report.md` (over-the-wire-limit body → `ExceededSizeLimit`). This
> report covers the **declared-size / pre-allocation** axis.

---

## 1. Objective

Probe whether a receiver **pre-allocates or gates on the advertised `eb_size`**
before fetching the body — a near-zero-cost buffer/OOM vector: declare a huge size,
serve an empty body. This is `FakeEbKind::Hollow { declared_bytes }` (designed in
leios-tools, but **not actuated** on net-node until this work — see §3).

## 2. Environment

| Component | Value |
|---|---|
| cardano-node | **w34**; `11.1.0.164`, git `3e1bec02`; consensus `3.0.1.0` |
| Committee | node1/2/3 (`:4001`/`:4002`/`:4003`), ~24T each |
| Adversary | net-rs (`:4010`), 4th pool, ~20% stake |

## 3. What was wired (net-node actuation)

leios-tools was already complete (`ActionSpec::HollowEb`, the `HollowEb` leaf →
`FakeEbKind::Hollow { declared_bytes }`, `EbSizePolicy`). Only the net-node arm was a
stub. Wired in `net-rs/net-node/src/production.rs`:
- Split the `Hollow | Loaded` stub; gave `Hollow { declared_bytes }` a real arm that
  produces an **empty** EB (`encode_overflow_eb(&[], …)`, `tx_count=0`, `eb_bodies=None`)
  and sets a new per-EB `ProducedEb.declared_size = Some(declared_bytes as u32)`.
- The announced-size computation now uses that override:
  `advertised = eb.declared_size.unwrap_or(announce_eb_size.apply(true_size))`.
- Behaviour `behaviours/{lib,attacks}/hollow-eb.bt` → `hollow-eb.toml`
  (`spec = { kind = "hollow-eb", declared_bytes }`), live via `GET /api/attacks`.

## 4. Sweep result

Declared-size sweep against the ~2.5 MB LeiosFetch wire limit:

| `declared_bytes` | vs wire limit | announcement | fetch/serve | RSS | teardown |
|---|---|---|---|---|---|
| **2,000,000** (2 MB) | below | **accepted** | empty body served, **no size error** | flat (~160 MB) | **none** |
| **2,000,000,000** (2 GB) | above | accepted | `MsgLeiosBlock size mismatch: (1,2000000000)` → `error` | bounded (~330 MB, never →2 GB) | `MuxErrored`→`ShutdownPeer` |

**Legend:** *wire limit* = the ~2.5 MB LeiosFetch message-size cap; *teardown* = the
peer connection is dropped; RSS is per Haskell node.

Two findings:
1. **No pre-allocation at any size.** RSS never approached the declared value (not
   2 GB, not 2 MB). The node fetches the tiny real body first, then reacts — it does
   **not** pre-size a buffer on the advertised `eb_size`. The buffer-exhaustion vector
   the probe targeted is **defended**.
2. **The teardown is gated on the wire limit, not the lie.** A below-limit declared
   size (2 MB) with an empty body is simply **accepted** — no error, no teardown. Only
   a declared size **above** the wire limit (2 GB) trips `MsgLeiosBlock size mismatch`,
   which is handled by an unhandled `error` (partial function) → connection teardown.
   That is the same class as the CBOR crash (`Leios.hs:283`) and the item-count vector
   (`Data.Vector`), and an extreme form of the Testnet-positive **EB Size Lie**
   (declared-vs-actual mismatch → connection reset).

## 5. Does it take down the network? (controlled confirmation run)

Re-run against a freshly-healed mesh (`declared_bytes=2 GB`), measuring block
production vs teardowns:

- **Per-EB teardown confirmed:** 2 Hollow EBs → **12 `size-mismatch` errors + 6
  `ShutdownPeer`**. Each malicious EB reliably drops connections.
- **No sustained network takedown at 20% stake:** the chain kept advancing (**+18
  blocks over the run, ~baseline rate**). Two reasons:
  1. **Self-throttling** — net-rs produced only 2 Hollow EBs in ~4.5 min (far below
     its ~1/100 s forge rate) because the teardowns cost net-rs its *own* downstream
     consumers, blocking its forge gate.
  2. The mesh **re-forms between bursts** faster than net-rs emits new Hollow EBs.
- **Correction:** an earlier *uncontrolled* run appeared to stall the chain; that was
  confounded by a simultaneous net-rs relaunch/churn, not the attack alone. The clean
  controlled run shows the network is **resilient** to one-EB-per-RB Hollow at
  minority stake.

**To sustain a takedown** you would need Hollow/size-lie EBs faster than the mesh
recovers — a **per-tick flood** (like `announce-flood`) or materially higher stake.
The current one-EB-per-RB rate self-limits.

## 6. Reproduction

```bash
# below limit — accepted, harmless:
curl -X POST http://127.0.0.1:9100/api/attack -H 'content-type: application/json' \
 -d '{"id":"hollow-eb","selection":{"kind":"all"},
      "parameters":[{"toml_path":"behaviours.hollow-eb.spec.declared_bytes","value":2000000}]}'
# above limit — size-mismatch error + peer teardown:
#   ...same, value: 2000000000
grep -E "size mismatch|ShutdownPeer" ~/leios-proto-devnet/node{1,2,3}/node.log
curl -X POST http://127.0.0.1:9100/api/attack/stop
```

Sample RSS per node (`ps -o rss=`) throughout — it stays bounded regardless of
`declared_bytes` (the pre-allocation negative result).

## 7. Recovery

Disarm. At 20% stake the mesh self-recovers between bursts. If a run leaves the mesh
degraded (heavy churn), restart the Haskell nodes from their DBs under `caffeinate`
(no respin, genesis untouched) to re-form the mesh cleanly.

## 8. Root cause & recommendation

- **Pre-allocation:** correctly bounded — no fix needed on this axis.
- **Size-mismatch handling** at `MsgLeiosBlock` uses a partial `error` on untrusted
  input (the declared-vs-actual / over-limit case). Replace with a rejectable decode
  error: decline the block, drop/penalize the peer — do not `error`. (Same fix pattern
  as `Leios.hs:283` and the 13,888 vector.)

## 9. Open items

- **Flood escalation:** wire a per-tick Hollow/size-lie flood to test sustained
  network takedown (the one-EB-per-RB rate self-limits at 20%).
- Whether the ~2.5 MB gate is exactly the LeiosFetch wire limit, and whether any
  declared value in `(actual, wire-limit]` drives a *bounded* pre-alloc (RSS showed
  none at 2 MB, but a finer sweep would confirm the whole range).
- Loaded (real-tx EB) remains unwired — orthogonal to this axis.
