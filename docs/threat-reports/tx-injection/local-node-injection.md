# Injecting real txs into a live Haskell node via `net-node`

**Status: working, verified 2026-08-11.** An honest magazine tx built by
an honest magazine tx (built by our internal magazine tooling) was injected through `net-node --tx-source` into our local
Leios Haskell `cardano-node` and **landed on the musashi chain**
(tx `96080651…` at a team-funded address; node logged `TraceMempoolAddedTx` → block).

This is the first end-to-end proof that the tx-injection tooling works against a
**real** node (not just a net-node↔net-node cluster). It unblocks bead `9zq.4`.

---

## The one thing that makes or breaks it

A Haskell `cardano-node` **does not pull transactions from an unsolicited inbound
peer.** If `net-node` merely dials the node, the connection is fine and net-node
loads the magazine into its mempool, but the node **never requests the txs** and
nothing lands. Verified twice — with `net-node --tx-source` and with `net-cli
submit` (both connect, both go idle, zero `TxSubmission` events for `127.0.0.1`,
zero landings). The node runs classic `TxSubmission` only with peers in its
**topology/peer-set**, not with random inbound dialers.

The fix is to make `net-node` a **trustable local-root peer** so the node
maintains a hot connection to it and runs `TxSubmission` over it. In this run the
tx was pulled **only after** the node dialed net-node via the local root
(`peer-1`), ~18 s after net-node had already generated it on its own outbound
connection (`peer-0`).

---

## Recipe

(The commands below reference our internal tooling, which is not published; the
mechanism and the node configuration are what matter.)

### 1. net-node config — listen *and* dial

net-node must **listen** (so the node's local-root dial lands) and also **dial**
the node (for chain sync, so it's a valid peer). Overlay on the dev config
(network magic 164): a dedicated node id, **zero stake**, a low tx rate, and one
peer entry dialing the local Haskell node for chain sync.

### 2. Node topology — add net-node as a trustable local root

Edit the Haskell node's `config/topology.json`, then **restart the node** to read
it (topology is read at startup):

```json
"localRoots": [
  { "accessPoints": [{ "address": "127.0.0.1", "port": 3020 }],
    "advertise": false, "trustable": true, "valency": 1 }
]
```

### 3. Build a FRESH magazine every run — mind the cursor

`net-node --tx-source` writes a `<magazine>.cursor` sidecar and **resumes from
it**. A reused magazine replays *nothing* — net-node logs
`tx magazine exhausted; generator idle tx_count=0` and offers no tx (this cost us
a full debugging cycle). Either build to a new path or delete the `.cursor`.

Magazines are also **perishable**: each spends a specific UTxO, so rebuild after
anything that spends it (the build queries the live UTxO each time).

### 4. Run — order matters

Restart the node (step 2) **first** and let it come up; *then* start net-node.
Restarting the node *after* net-node connects tears down net-node's connection
(the "mux shut down" we saw was a mistimed restart, not an interop bug).

Run net-node with the base dev config, the local-inject overlay and the magazine
as `--tx-source`. The node establishes the local-root duplex connection and pulls
net-node's txs via `TxSubmission`. Landing took ~80 s here (establish
local-root → pull → one block). Verify on-chain with
`cardano-cli dijkstra query utxo` for the target address.

---

## Why it works (direction)

`TxSubmission` is pull-based: the **client is the tx provider** (sends `MsgInit`),
the **server is the consumer** (drives requests). The connection *initiator* runs
the client. So:

- net-node's **outbound** connection to the node (net-node = initiator = provider)
  is the direction that delivers our txs.
- The node only engages `TxSubmission`-consumer over that connection once net-node
  is an **established local-root peer** — which requires the node to also know and
  dial net-node (hence `listen_address` + the `localRoots` entry), forming a hot
  duplex connection over which both directions of `TxSubmission` run.

---

## Gotchas checklist

- [ ] net-node **not** a local root → node never requests txs, nothing lands.
- [ ] Stale `<magazine>.cursor` → net-node replays nothing (`tx_count=0`).
- [ ] Restarting the node **after** net-node connects → tears the connection down.
- [ ] Spent UTxO → the magazine tx is invalid; rebuild.
- [ ] This `cardano-cli` build has **no `transaction view`** subcommand.

---

## Local vs. testnet targets

The local node gives full log observability (mempool add/reject, `TxSubmission`),
which is why it's the right first target. The usual testnet IPs "known to speak to
net-rs" presumably already list net-rs peers in their topology (the same local-root
mechanism), so the same recipe should apply there — minus the observability. Only
a multi-node network shows the flood-level effects (T31 displacement, propagation).
