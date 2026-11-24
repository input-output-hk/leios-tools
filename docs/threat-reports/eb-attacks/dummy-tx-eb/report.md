# Dummy Tx EB → real Haskell node crash (liveness/DoS)

**Date:** 2026-09-02
**Environment:** proto-devnet (local), 3 Haskell BP nodes + net-rs (Piranha) 4th pool
**Attack:** `dummy-tx-eb` (fake-EB family, variant B), driven live via `POST /api/attack`
**Target node:** **cardano-node w34** (`prototype-2026w34` release tag; `11.1.0.164`,
git `3e1bec02`, `ouroboros-consensus-3.0.1.0`).
**Verdict:** **positive — remotely-triggerable node crash. MAJOR FINDING.** A
fabricated EB whose transaction bodies are servable but not valid Leios-tx CBOR
crashes committee nodes that process it. **Reproduced across 2 independent runs;
both took down the same two of three nodes** (§5d).

> Severity note: this is a **liveness/denial-of-service** finding (honest nodes
> crash), which is more severe than the soundness question the run set out to test.
> Root cause is a partial `error` on the deserialization of untrusted network data.
> Verification evidence package: the internal `crash-repro-2026-09-02/` bundle
> (see its `VERIFICATION.md`); retained internally, not published.

---

## 1. Summary

While testing whether the real Haskell committee would certify a fabricated
endorser block (it does — see §5), two of the three committee nodes **crashed**
with a fatal Leios-tx deserialization error. The fabricated transactions in the
dummy EB are servable CBOR (so the availability/`MissingTX` gate passes and the
committee votes/certifies), but their **body element is a random CBOR bytestring
where a transaction-body map is required**. When a committee node subsequently
deserializes that transaction — in its forge loop and/or its trace renderer — the
decoder fails and `Ouroboros.Consensus.Shelley.Ledger.Leios.hs:283` handles the
failure with `error`, an unrecoverable panic. The linked forge thread dies, the
diffusion layer errors out, and the process exits.

## 2. Environment

| Component | Value |
|---|---|
| cardano-node | **w34** (`prototype-2026w34`); `11.1.0.164`, darwin-aarch64, ghc-9.6, git `3e1bec02` |
| consensus | `ouroboros-consensus-3.0.1.0` |
| Committee | 3 Haskell BP nodes (`node1`/`node2`/`node3`), ~80% stake total (24T each) |
| Adversary | net-rs (Piranha), 4th pool, 18T (~20%) stake, seat 0 |
| net-rs config | `certify_own_ebs = false`, `rb_body_max_bytes = 0`, `provide_txs = false` |
| Control plane | net-cluster attack API on `:9100` |

**Legend:** *seat* = voter index in the committee (net-rs = 0, Haskell = 1/2/3);
*T* = trillion lovelace; *certify_own_ebs=false* = net-rs never carries a cert for
its own EB (a Haskell RB must), so all certification observed is the real committee.

## 3. Nature of the fake EB and its exact contents (for reproduction)

The `dummy-tx-eb` behaviour sets `PraosControl.fake_eb = Some(FakeEbKind::Dummy { n_txs })`
with **`n_txs = 8`** by default (`net-rs/net-cluster/behaviours/dummy-tx-eb.toml`).
On every ranking block net-rs produces, `production.rs` builds an EB whose manifest
lists `n_txs` fabricated transactions and pins their bodies so they are servable.

**Each fabricated transaction** (`net-node/src/mempool.rs::make_fake_tx_bytes`,
`DUMMY_TX_SIZE = 256` bytes):

```
CBOR = array(4) [
  bytes(<250 random bytes>),   # tx BODY element — a bytestring, NOT a tx-body map
  map(0),                      # witness_set (empty)
  true,                        # is_valid
  null                         # aux_data
]
```

- `wire_tx_id` = hash of the body element → a well-defined tx id, so the manifest
  hashes are consistent and fetchable.
- Bodies are pinned via `Mempool::merge_eb_body` into `eb_pinned`; net-rs's
  `TxBodyResolver` serves them on `LeiosFetch BlockTxs`. **Availability is satisfied
  — voters never raise `MissingTX`.**
- **The defect trigger:** the outer array is a valid 4-element Shelley+ tx envelope,
  but element 0 (the body) is a random *bytestring*. The Haskell Leios-tx decoder
  reads the array, then expects the body to be a CBOR **map** and finds a bytestring
  → `DeserialiseFailure 1 "expected map len or indef"`.

The EB is announced normally on net-rs's produced RB (`announced_eb`), with 8 such
transactions, `eb_hash` e.g. `e557e7ef…` (slot 5953 in the first run).

## 4. Pipeline and where it breaks

1. **Announce.** net-rs forges an RB announcing a dummy EB (8 fabricated txs).
2. **Fetch.** Committee fetches the EB manifest and all 8 tx bodies (served from
   net-rs's pin store). Availability passes — no `MissingTX`.
3. **Vote / certify.** Committee votes on availability (no tx-content validation at
   this gate) and reaches quorum → `LeiosCertified`. Confirmed §5.
4. **Process → crash.** When a committee node later deserializes one of the
   fabricated Leios txs (forge-loop ledger tick and/or trace rendering), the decoder
   fails and `Leios.hs:283` calls `error` → panic → `ExceptionInLinkedThread` →
   `DiffusionErrored` → the process exits.

The crash is **probabilistic in timing**: a 4-minute run (3 dummy EBs, run 1) did
*not* crash any node; a 19-minute run (10 dummy EBs, run 2) crashed two. It fires
when a committee node hits the forge/trace path with a fabricated tx in its Leios
state.

## 5. Evidence

### 5a. The crash (node1 log, `node.log.crash-151102`)

```
Error rendering trace message: Failed to deserialise Leios tx:
  DecoderErrorDeserialiseFailure "Leios Tx" (DeserialiseFailure 1 "expected map len or indef")
  error, called at ouroboros-consensus-cardano/src/shelley/Ouroboros/Consensus/Shelley/Ledger/Leios.hs:283:17
    in ouroboros-consensus-3.0.1.0-…:Ouroboros.Consensus.Shelley.Ledger.Leios

{"at":"2026-09-02T20:05:52.009806Z","ns":"Startup.DiffusionInit.DiffusionErrored",
 "data":{"error":"ExceptionInLinkedThread \"ThreadId 54\" Failed to deserialise Leios tx: …"}}

cardano-node: ExceptionInLinkedThread "ThreadId 54" Failed to deserialise Leios tx:
  DecoderErrorDeserialiseFailure "Leios Tx" (DeserialiseFailure 1 "expected map len or indef")
  error, called at …/Leios.hs:283:17
```

The last healthy forge-loop trace immediately before the panic was a
`Forge.Loop.Call name=get-ticked-ledger-state` at `20:05:52.002964Z` — i.e. the
crash is on the forge/ledger path.

| Node | Crashed at | Result |
|---|---|---|
| node1 (:4001) | 2026-09-02T20:05:52Z | process exit |
| node2 (:4002) | 2026-09-02T20:06:50Z | process exit |
| node3 (:4003) | — | survived (did not hit the forge path with the bad tx) |

### 5b. Certification of the fabricated EBs (the availability-only voting gate)

- **Run 1 (17:32–17:37):** 3 dummy EBs forged, **3/3 certified** by the Haskell
  committee. All three voters (seats 1/2/3) logged `LeiosVoteAcquired` then
  `Consensus.LeiosKernel.Certified`. Full-hash verified, e.g.
  `50616d5cc6f3974e46081197d3281ff3549296c1354fd0bd6ea1814caecbb768`. `missing_tx_rate`
  stayed ~0 (bodies were servable).
- **Run 2 (19:56–20:15, the crash run):** 10 dummy EBs forged, **3 certified**
  (`5f65d56b`, `08db0936`, `0441b634`); none of their certs were embedded (net-rs had
  `certify_own_ebs=false`), and no honest producer carried them — see the embed
  analysis below — before two nodes crashed.

### 5c. Honest producers refuse to embed the dummy cert (secondary finding)

With `certify_own_ebs=false`, embedding a dummy cert requires a Haskell node to
forge the immediate child of the dummy-EB RB and carry the cert. For dummy EB
`08db0936`, node1 formed the cert at `20:00:51.025`, then forged the only eligible
child block (blockNo 727) at `20:00:52.004` — **~980 ms later, holding the cert —
and omitted it.** Contrast the honest baseline, where Haskell nodes embedded
net-rs's real EB certs readily (5 embeds earlier the same day). This indicates the
honest block producer validates EB tx content before carrying its cert and refuses
for fabricated EBs — i.e. building a block that applies the txs is where content is
checked. (The crash pre-empts this defense: nodes die before/around the embed.)

### 5d. Reproduction — a controlled verification run (2026-09-02, w34)

Repeated as a controlled experiment with a clean negative control. Full evidence
and a step-by-step timeline are in the internal `crash-repro-2026-09-02/VERIFICATION.md`.

| # | when | node1 crash | node2 crash | node3 |
|---|---|---|---|---|
| Run A (first occurrence) | ~19:56–20:07Z | 20:05:52Z | 20:06:50Z | survived |
| Run B (verification) | armed 22:25:37Z | 22:26:20Z | 22:27:09Z | survived |

Run B specifics: **arm → first crash ≈ 43 s**; both nodes died with the byte-identical
error and code location (`Leios.hs:283`) on the forge path (`get-ticked-ledger-state`,
`ThreadId 55` → `DiffusionErrored`). Negative control: **0** `Failed to deserialise`
errors and all three nodes healthy before arming
(recorded in the internal evidence bundle). Crash excerpts for both nodes and both
runs are retained in the same internal bundle. Timing between
runs varies (≈10 min vs ≈43 s) — it is probabilistic, firing when a committee node
reaches the forge/ledger path with a fabricated tx in state — but the crash itself is
deterministic once hit.

## 6. Reproduction

Prerequisites: a healthy proto-devnet (3 Haskell nodes + net-rs), attack API on
`:9100`, net-rs honest (`certify_own_ebs=false`).

```bash
# 1. Arm the fake EB on net-rs (node 0), default n_txs=8:
curl -s -X POST http://127.0.0.1:9100/api/attack \
  -H 'content-type: application/json' \
  -d '{"id":"dummy-tx-eb","selection":{"kind":"all"}}'

# 2. Let net-rs forge several dummy EBs (~1 per 90 s). Leave it armed ~15–20 min;
#    the crash is timing-probabilistic (needs a committee node on the forge/trace
#    path with a fabricated tx in state). Watch:
tail -f ~/leios-proto-devnet/node{1,2,3}/node.log \
  | grep -E "Failed to deserialise Leios tx|Leios.hs:283|DiffusionErrored"

# 3. Confirm certification (availability-only gate) in the Haskell logs:
grep -h "LeiosCertified" ~/leios-proto-devnet/node*/node.log | tail

# 4. Disarm:
curl -s -X POST http://127.0.0.1:9100/api/attack/stop
```

Expected: one or more `node.log` files end with `Failed to deserialise Leios tx …
Leios.hs:283 … ExceptionInLinkedThread … DiffusionErrored` and the process exits.

## 7. Recovery (validated)

Restart each crashed Haskell node from its **existing** database (no genesis reset);
they resume cleanly because the offending tx lived in in-memory Leios state, not on
disk. Wrap in `caffeinate -i` for good measure:

```bash
cd ~/leios-proto-devnet/node1   # repeat for node2
LEIOS_DB_PATH="leios.db" nohup caffeinate -i cardano-node run \
  --config config.yaml --topology topology.json --database-path db \
  --socket-path node.socket --host-addr 127.0.0.1 --port 4001 \
  --shelley-kes-key keys/kes.skey --shelley-vrf-key keys/vrf.skey \
  --shelley-operational-certificate keys/opcert.cert --shelley-bls-key keys/bls.skey \
  > node.log 2>&1 &
```

After recovery: all 3 nodes forging, chain advancing, net-rs honest, attack disarmed.

## 8. Root cause and recommended fix

`Ouroboros.Consensus.Shelley.Ledger.Leios.hs:283` responds to a Leios-tx
deserialization failure with `error` (a partial function / panic) on data that
originates from the network. Untrusted input that fails to decode must be handled
as a rejectable error (decline the tx/EB, drop the peer), never as a fatal
assertion. This is a decoder-hardening / availability bug independent of the
soundness question.

## 9. Open questions

1. **Generality.** Is this reachable by *any* malformed Leios tx from *any* peer
   (general decoder-hardening gap), or only via the specific `make_fake_tx` CBOR
   shape (a net-rs ↔ Haskell wire-format detail)? The `error`-on-decode is a bug
   either way; this decides blast radius. → next experiment: minimal single-EB repro
   and a byte-level check of what the decoder rejects.
2. **Trigger point.** Confirm whether fetch-for-voting alone can trigger it, or
   whether it requires the tx to reach the forge/ledger path (evidence points to the
   latter).
3. **Soundness (now moot on this build).** The `certify_own_ebs=true` escalation
   (net-rs embeds the committee cert for its own fabricated EB and offers it, to test
   the adoption/application gate) is pre-empted by the crash — nodes die before an
   adoption/fork decision. Revisit only if the crash is fixed.

## 10. Artifacts

- Crash logs: `~/leios-proto-devnet/node1/node.log.crash-151102`,
  `~/leios-proto-devnet/node2/node.log.crash-*`.
- Attack definition: `net-rs/net-cluster/behaviours/dummy-tx-eb.toml`
  (`FakeEbKind::Dummy { n_txs: 8 }`).
- Fabricated-tx encoder: `net-rs/net-node/src/mempool.rs::make_fake_tx_bytes`
  (`DUMMY_TX_SIZE = 256`).
- Family design: [`../PLAN.md`](../PLAN.md) (variant B, Dummy Tx EB).

---

## 11. Testnet confirmation — musashi, live IOG committee (2026-09-03)

**Verdict: positive — the same crash, reproduced against the real shared Leios
testnet and IOG's production committee, through an honest relay.** The §1–§10 run
above was proto-devnet with net-rs peered **directly** to the committee (one hop).
This section records the follow-on run on **musashi** (the shared Leios testnet),
where net-rs sits **behind the local `musashi-relay` Haskell node** and reaches the
IOG committee only through it — the realistic production topology — and documents
the one net-rs change required to make the poisoned EB diffuse past a relay.

**Environment:** musashi testnet; net-rs (Piranha, pool `PIR0`, ~19.7% active stake)
→ local `musashi-relay` (Haskell `cardano-node`, `prototype-2026w35`) → public
musashi swarm incl. the IOG committee. `certify_own_ebs = true` here (self-cert, so
the fabricated EB carries a cert and diffuses out through the relay). Attack armed
live: `POST /api/attack {"id":"dummy-tx-eb","selection":{"kind":"all"}}` (behaviour-
tree swap, no node restart).

### 11a. Why the relay initially blocked it (the earlier false negative)

An earlier musashi attempt (2026-08-27) was logged as **negative** — the fabricated
EB did not certify. That was **not** the real network validating and rejecting the
content: it was a **diffusion gap**. net-rs announced the fake EB (as the on-chain
reference in the RB it forges, diffused via ChainSync) and gossiped the availability
*announcement*, but **never made the LeiosNotify body/tx offer** (`MsgLeiosBlockOffer`
/ `MsgLeiosBlockTxsOffer`) for its own self-produced EB. LeiosFetch is **offer-
driven** — a peer issues a `BlockRequest`/`BlockTxsRequest` only for a body another
peer has *offered*. With no offer, the relay counted the EB among `missingBodies`
but issued **zero** fetch requests, never obtained the body, and never re-diffused
it; the poison stalled one hop from the committee. Confirmed on the relay:
`Consensus.LeiosKernel.AnnouncementAccepted source=receivedViaChainSync`;
`FetchDecision` with `missingBodies>0`, `decisionBodyRequests=0`; and **zero**
`BlockOffer`/`BlockTxsOffer` for any fabricated hash.

(On the proto-devnet run of §1–§10, net-rs was peered **directly** to the committee,
so its serve-on-request path answered committee fetches with no relay in between —
which is why the crash reproduced there without this fix.)

### 11b. The fix — net-rs now offers its fabricated EB (`net-node` only)

- **`production.rs`:** `ProducedEb` gains `eb_bodies: Option<Vec<TxBody>>`. The
  `Dummy` branch fabricates the `n_txs` bodies once and carries them; `Phantom`
  (unfetchable by design) and honest overflow EBs stay `None`.
- **`main.rs`:** the self-produced-EB handler injects the carried bodies straight
  into the `LeiosStore` via `inject_block_txs_full`, which enqueues the
  `BlockTxsOffer` and makes the txs fetch-servable. The producer now **makes the
  offer** instead of relying on a mempool re-resolution that never succeeds for
  fabricated txs (the old path logged `EB will be unfetchable`).
- Unit test: `dummy_fake_eb_carries_servable_bodies_matching_manifest`;
  `cargo test -p net-node` → 201 passed. `leios-tools` untouched (CI stays green
  against `leios-tools` main).

### 11c. End-to-end diffusion through the relay (evidence)

For each fabricated EB (e.g. slot 2342922, EB `fe57ed2c…`, 8 dummy txs, 297-byte
body):

net-node:
```
production: announcing DUMMY Tx EB (servable fabricated txs) slot=2342922 n_txs=8
net_node:   offered self-produced fake-EB bodies for LeiosFetch serving n=8
peer_task:  leios_notify: EB offered      eb_hash=fe57ed2c… eb_bytes=297
peer_task:  leios_notify: EB txs offered  eb_hash=fe57ed2c…
server_handlers: leios_fetch: downstream requested EB body from us → serving EB body body_bytes=297
server_handlers: leios_fetch: downstream requested EB txs from us  → retrieved transactions from store requested=8 actual=8
```

relay (`journalctl -u musashi-relay`):
```
LeiosFetch.Send.BlockRequest      ebHash=fe57ed2c… ebSlot=2342922
LeiosFetch.Send.BlockTxsRequest   ebHash=fe57ed2c… numTxs=8 bitmap=0xff00000000000000
Consensus.LeiosKernel.BlockAcquired / BlockTxsAcquired / BodyHits
LeiosFetch.Send.Block ×21   LeiosFetch.Send.BlockTxs ×19   (re-offered + served onward)
```

The offer→fetch→serve→re-diffuse chain that was broken now completes: the relay
fetches the fabricated body+txs from us, acquires them, and re-diffuses them to the
wider committee. Before the fix the relay received the announcement but issued zero
fetch requests; after it, `Send.BlockRequest` + `Send.BlockTxsRequest` and full
acquisition.

### 11d. Delivery to the IOG pools

IOG1 / IOG2 / IOG3 (hex ids `b133eabca09fe76539d304a8048fdb7981387ad67c1e5ce2e39e64be`
/ `5f948c32c648862108cba5370514738e64c3e88209f1909ec7cefe54` /
`97e5f28ed2059f98c03d6a9af5f308dfea0c8240ce0d6515f26cd5f3`) share **one** registered
relay, `leios-node.play.dev.cardano.org:3001` — which is also `musashi-relay`'s
configured upstream peer (`topology.json`). For **all four** fabricated EBs of the
run (`a9d65290…`, `b80dfb4f…`, `f72ed98f…`, `fe57ed2c…`), our relay served **both
the EB body and all 8 fabricated txs** to that relay, observed as
`LeiosFetch.Remote.Send.Block` + `Send.BlockTxs` to that relay's hostname
on the exact registered port, pull-initiated by the IOG relay. Our
vantage proves delivery to the IOG *relay* fronting the three block producers; the
final relay→BP hop is inside IOG's network.

### 11e. Impact — live crash on production IOG nodes

Deserializing the fabricated txs **crashed several live IOG committee nodes**,
confirmed by the IOG **SRE team** — the same `Leios.hs:283` CBOR-deserialize root
cause as §5/§8. The run was **pre-coordinated with SRE** so they were ready, armed
via the attack API, **disarmed** immediately after confirmation, and the affected
nodes were **restored**. The offending CBOR snippet was shared with the developers
to address the defect.

### 11f. Significance

This elevates the finding from a **local proto-devnet** crash (§1–§10) to a crash
**reproduced against the real shared testnet and IOG's production committee, through
an honest relay**, triggered by a **sub-25% adversary** (PIR0, ~19.7% stake). The
relay is not a mitigation: once the adversary offers the body, an honest relay
fetches it and re-diffuses the poison onward to the committee. The root cause and
recommended fix are unchanged from §8 — the Leios-tx decoder must **reject**
malformed untrusted input, not `error` on it.

**Code / commit:** `net-node` changes on branch `ct/dummy-tx-eb-attack`
(`net-rs/net-node/src/production.rs`, `net-rs/net-node/src/main.rs`); tracker updated
in [`../../attack-results.md`](../../attack-results.md) (Dummy Tx EB Testnet verdict → **positive**).
