# Oversized EB (13,889-item manifest) → LeiosFetch vector overflow & peer disconnect

**Date:** 2026-09-03
**Environment:** proto-devnet (local), 3 Haskell BP nodes + net-rs (Piranha) 4th pool
**Attack:** `phantom-tx-eb` with `n_txs = 13889` (item-count override; no new code)
**Target:** cardano-node **w34** (`prototype-2026w34`; `11.1.0.164`, git `3e1bec02`, `ouroboros-consensus-3.0.1.0`)
**Verdict:** **confirmed.** An EB whose manifest carries more items than the node's
per-EB vector capacity (13,888) triggers `index out of bounds (13888,13888)` in the
**LeiosFetch (mini-protocol 19)** serving path. It **tears down the peer connection**
(`ShutdownPeer`) but does **not** crash the node; sustained, the connection churn
**stalls the chain**, which **self-recovers when the attack stops**.

> This is a **distinct** defect from the malformed-CBOR node crash
> (`../dummy-tx-eb/report.md`, `Leios.hs:283/:308`): that one kills the
> node and needs a DB restart; this one is a connection-level fault that degrades
> the mesh but is recoverable by stopping the attack.

---

## 1. Summary

net-rs forges an Endorser Block whose manifest lists **13,889** transaction hashes —
one more than the receiving node's fixed per-EB vector capacity of **13,888**. The EB
is accepted at the fetch/outer layer (self-consistent size + hash, under the generic
wire limit), persisted, and then when a node **serves** it over LeiosFetch, the code
writes rows into a mutable vector sized for 13,888 entries. The write at index 13,888
raises `index out of bounds (13888,13888)`. The exception propagates to the mux and
the connection manager, which **shuts down the peer** (`ResponderErrored` →
`MuxErrored` → `ShutdownPeer`). The node's supervision isolates the fault to the
connection, so the process survives, but the repeated teardowns across the mesh
**pause block production** for the duration of the attack.

This is the "Mega Tx EB" variant from [`../PLAN.md`](../PLAN.md) (planned, never built
as a dedicated `FakeEbKind`). It is **reachable today** through `phantom-tx-eb`
because net-rs clamps `n_txs` against its own `eb_body_max_bytes`, not the Haskell
node's item limit — see §3.

## 2. Environment

| Component | Value |
|---|---|
| cardano-node | **w34** (`prototype-2026w34`); `11.1.0.164`, ghc-9.6, git `3e1bec02` |
| consensus | `ouroboros-consensus-3.0.1.0` |
| Committee | node1/2/3 (`:4001`/`:4002`/`:4003`), ~24T stake each |
| Adversary | net-rs (`:4010`), 4th pool, ~20% stake, `certify_own_ebs=false` |
| Control plane | net-cluster attack API on `:9100` |

## 3. Why the oversized EB can be produced (net-rs side)

- The dedicated "Mega Tx EB" attack is **not implemented**: `FakeEbKind` =
  `Phantom | Dummy | Hollow | Loaded` (Hollow/Loaded are not actuated on this build;
  Mega is doc-only). No `mega`/`huge` behaviour exists.
- But `phantom-tx-eb` builds a manifest of `n_txs` random 32-byte hashes, and its
  clamp is against **net-rs's own** cap: `max_txs = eb_body_max_bytes / 32 =
  16,384,000 / 32 = 512,000` (`production.rs`). `13,889 ≪ 512,000`, so it is **not
  clamped**.
- `n_txs` is a live-settable parameter (`behaviours.phantom-tx-eb.spec.n_txs`), so a
  single `POST /api/attack` with a parameter override produces the 13,889-item EB. Its
  body is ~0.5 MB — well under the assessed ~2.5 MB generic wire limit, and the
  per-EB item cap (13,888) is a **separate** limit the fetch/ingest path does not
  enforce.

Phantom (not Dummy) is the clean choice: its manifest is **valid CBOR** — the only
violation is the item count — so the fault is unambiguously the vector overflow, not
a content-decode error.

## 4. Pipeline and where it breaks

1. **Produce.** net-rs forges an RB announcing a 13,889-item phantom EB.
2. **Fetch/ingest.** A committee node fetches the EB body; ingestion checks only that
   the body matches the advertised size and hash (both self-consistent). It does
   **not** reject a self-consistent body above the per-EB item limit. The EB is
   accepted and persisted.
3. **Serve (crash point).** When the node serves that EB over **LeiosFetch
   (mini-protocol 19)**, it writes rows into a vector allocated for 13,888 entries.
   The write at index 13,888 raises `index out of bounds (13888,13888)`
   (`src/Data/Vector`).
4. **Teardown.** The exception surfaces as `Net.InboundGovernor.Remote.ResponderErrored`
   / `Net.Mux.Remote.MuxErrored`, and the connection manager issues `ShutdownPeer`
   (both `Inbound` and `Outbound` contexts) — the **entire peer connection** drops,
   not just the EB protocol.

## 5. Evidence

Armed `phantom-tx-eb n_txs=13889` at **2026-09-03T17:10:39Z** (baseline: 0 bounds
errors on all three nodes). Representative log line (node2), verbatim:

```
{"ns":"Net.Mux.Remote.ExceptionExit",
 "data":{"bearer":{"connectionId":"127.0.0.1:4002 127.0.0.1:4010"},
 "event":{"exception":"index out of bounds (13888,13888)\n
   CallStack (from HasCallStack):\n  error, called at src/Data/Vector..."}}}

{"ns":"Net.InboundGovernor.Remote.ResponderErrored",
 "data":{"connectionId":{"localAddress":{"port":"4002"},"remoteAddress":{"port":"4010"}},
 "miniProtocolNum":{"kind":"MiniProtocolNum","num":19}}}

{"ns":"Net.ConnectionManager.Remote.ConnectionHandler.Error",
 "data":{"connectionHandler":{"command":"ShutdownPeer","context":"OutboundError",
 "reason":"index out of bounds (13888,13888) ..."}}}
```

- **Fault:** `index out of bounds (13888,13888)` — write at index 13,888 into a
  13,888-length vector — in `src/Data/Vector`.
- **Protocol:** `miniProtocolNum num=19` = **LeiosFetch** (the EB-serving path).
- **Bearer:** `:4002 ↔ :4010` — node2 processing the EB it fetched from net-rs
  (`:4010`); the fault also fired on node2↔node1 (`:4001`) and node2↔node3 (`:4003`).
- **Action:** `ShutdownPeer` (Inbound and Outbound) — full peer-connection teardown.
- **Counts (while armed):** node1 = 5, node2 = 15, node3 = 20 bounds errors.
- **No node crash:** `deserialise` errors stayed 0; all three processes stayed up.

## 6. Impact

| Aspect | Result |
|---|---|
| Node process | **Survives** — supervision isolates the exception to the connection |
| Peer connection | **Torn down** (`ShutdownPeer`) on every occurrence |
| Chain | **Stalled** while armed (blocks ~4212–4216 held) from cumulative churn |
| Recovery | **Self-heals on disarm** — no DB restart needed |

While the attack is armed, nodes repeatedly disconnect from each other (and from
net-rs) whenever the poisoned EB is served, and block production pauses — a
**network-liveness DoS by connection teardown**, not a node crash. It is milder than
the CBOR crash: stopping the attack is sufficient to recover.

## 7. Relationship to the external security analysis

The analysis predicted this precisely:

> "…Fetch uses a larger generic message limit and ingestion checks only that the body
> matches the size and hash… It never rejects a self-consistent body above the
> Leios-specific byte or item limit. A 13,889-item body… is persisted. The Fetch
> server later writes database rows into a mutable vector allocated for 13,888
> entries. The write at index 13,888 raises a bounds exception. This establishes
> termination of the affected serving session and persistence of an object violating
> its server invariant; **node-wide failure depends on unverified supervision**."

Confirmed:
- The `(13888,13888)` bounds exception fires on the serving path (LeiosFetch/19). ✔
- The serving session terminates and the peer connection is dropped. ✔
- **Node-wide failure does not occur on w34** — supervision catches it (the
  analysis's open dependency resolves to "no node death" on this build). ✔
- Added observation: the cumulative teardowns **stall the chain** while sustained,
  and it **self-recovers** once the oversized EBs stop.

Analysis-cited source (Haskell): `LeiosDemoTypes.hs:1759`, `LeiosDemoLogic.hs`,
`LeiosDemoOnlyTestFetch.hs:200`, `msgLeiosBlockRequest`. Our reproduction surfaces the
underlying fault as `src/Data/Vector` `index out of bounds (13888,13888)` on
mini-protocol 19.

Byte-size note: the analysis's 500,007-byte figure uses real txs with a per-entry size
value of 55; `phantom-tx-eb` uses a nominal per-entry size of 256, so the produced EB
is ~0.5 MB but not byte-identical. The trigger is the **item count** (13,889 vs the
13,888 vector), which is independent of the byte total.

## 8. Reproduction

```bash
# healthy proto-devnet (3 Haskell nodes + net-rs), attack API on :9100, net-rs honest
curl -X POST http://127.0.0.1:9100/api/attack -H 'content-type: application/json' \
 -d '{"id":"phantom-tx-eb","selection":{"kind":"all"},
      "parameters":[{"toml_path":"behaviours.phantom-tx-eb.spec.n_txs","value":13889}]}'

# watch the committee logs for the fault on mini-protocol 19:
grep -E "index out of bounds \(13888|ResponderErrored|ShutdownPeer" \
  ~/leios-proto-devnet/node{1,2,3}/node.log

curl -X POST http://127.0.0.1:9100/api/attack/stop     # disarm -> chain self-recovers
```

Expected: `index out of bounds (13888,13888)` on `miniProtocolNum 19`, `ShutdownPeer`
teardowns, a chain stall while armed, and self-recovery on disarm. Nodes stay up.

## 9. Recovery

Disarm the attack. net-rs stops forging oversized EBs, the bounds errors stop
(they plateaued — no persistent re-error loop), peer connections re-establish, and the
chain resumes within ~1–2 minutes. No node restart or respin required.

## 10. Root cause and recommended fix

Two gaps compound:
1. **Ingestion does not enforce the per-EB item (or byte) limit** — a self-consistent
   body above the Leios-specific limit is accepted and persisted, provided it is under
   the generic wire limit. Reject an EB whose manifest exceeds the per-EB item/byte
   limit at fetch/ingest, before persistence.
2. **The serving path writes into a fixed 13,888-entry vector without a bounds guard**,
   so an over-limit object that slipped past (1) crashes the serving session with a
   partial `error`. Bound-check the write (or size the vector to the validated length)
   and handle the over-limit case as a rejectable error, not a session-killing
   exception.

Either fix alone closes the crash; both are worth doing (defense in depth: reject at
ingest, and never let untrusted data drive an unchecked vector index).

## 11. Notes / open items

- **Dedicated attack:** consider wiring a first-class `Mega`/`oversized-eb` behaviour
  (with `n_items` / declared-size knobs) so this is a named, repeatable catalogue
  entry rather than a `phantom-tx-eb` parameter override.
- **Real-tx variant:** `Loaded` (real txs from a magazine) is not actuated on this
  build; a real-tx oversized EB (to match the analysis's exact 500,007-byte scenario)
  would go via the honest-overflow path (~14k small real txs into one EB) or by wiring
  `Loaded`. Not required to confirm the bug — phantom isolates the item-count path
  cleanly.
- **Supervision dependency:** node survival here relies on the per-connection
  supervision catching the exception. A build or configuration where the fault
  propagates past that boundary would escalate this from connection-teardown to
  node-crash — worth checking against other builds.
