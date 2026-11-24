# tx-injection tools

Offline tooling for the transaction-flooding attack family (internal; the
scripts themselves are not published). Design and sizing math live in
[`../PLAN.md`](../PLAN.md); the node-side replayer is the node's `tx_source`
module (`--tx-source`).

| Tool | Purpose | Needs testnet? |
|------|---------|----------------|
| `sizing.py` | `k·R·T/C` calculator — UTxO counts, fees, reference points | no |
| `magazine.py` | build / inspect magazine files (garbage or signed txs) | no |
| `make-magazine.sh` | build the cheap real-tx magazines — T24 duplicate, T26 conflicting (single UTxO, no fan-out) | **yes** |
| `prep-magazine.sh` | fan-out + pre-sign disjoint real testnet txs — T31 load | **yes** |
| `build-honest-magazine.sh` | one command: valid INDEPENDENT self-transfers (auto multi-fan-out) for honest EB-only injection — dense EBs (internal how-to) | **yes** |
| `reclaim-funds.sh` | recover the ADA the flood spent (defrag pools + sweep the parking sink) back into a spendable UTxO | **yes** |

**Legend:** *UTxO* = unspent transaction output (a spendable input); *magazine* =
the flat file of pre-built txs a node replays via `--tx-source` (format below);
*T24* = duplicate-tx flood, *T26* = conflicting-tx (double-spend) flood, *T31* =
sustained distinct-tx load — the tx-injection threat IDs from [`../PLAN.md`](../PLAN.md).

**Which real-tx builder:** `make-magazine.sh` for the small dedup/conflict
probes (bead `9zq.4`); `prep-magazine.sh` for the sustained distinct-tx flood
(bead `9zq.6`). Both sign offline against the internal key store and pack via
`magazine.py`.

## Magazine format (v1)

A flat file of length-delimited records, no global header:

```
record := u32_le length N  ||  N bytes (one raw transaction, CBOR)
```

EOF follows the last record. The node rejects any length outside
`1..=2_500_000` (`MAX_TX_SIZE`) as corrupt. The node treats each blob as opaque
bytes, so the magazine's contents decide the threat:

| Contents | Threat | Environment |
|----------|--------|-------|
| garbage CBOR (`magazine.py garbage`) | T25 invalid | net-rs local |
| 1 signed tx, repeated | T24 duplicate | net-rs + testnet |
| 2+ signed txs, same input | T26 conflicting | net-rs + testnet |
| N signed txs, disjoint inputs | T31 load | net-rs + testnet |

**Legend:** *net-rs local* = local `net-node`/`net-cluster` (fake validation, so
garbage diffuses); *net-rs + testnet* = net-rs connected to the live
Leios/Dijkstra testnet (real ledger, garbage rejected at the first honest node).
Neither is the `sim-rs` simulator — these run on the net-rs stack only.

## Quick start

The sizing calculator reproduces the PLAN reference points and sizes a run
(`--tx-size`, `--duration`, `--k`, `--chain-depth`, `--nodes`). A net-rs-local
smoke flood needs only a garbage magazine (`magazine.py garbage`), no keys or
`cardano-cli`. Real testnet magazines need `cardano-cli`, the node socket and the
key store: `make-magazine.sh` builds the cheap T24-duplicate / T26-conflict
probes from a single funded UTxO, and `prep-magazine.sh` fans out and pre-signs
the sustained distinct-tx flood (`--width` lanes × `--chain-depth` chain,
`--submit` broadcasts the fan-out).

## Reclaiming spent funds

A flood run doesn't destroy the pool ADA — it *scatters* it. `prep-magazine.sh`
fans the big pool UTxO into many small lanes; non-final chain hops return
fragments to the flood address, and the final hop of every lane drains its
remainder into the shared **parking** sink — an enterprise (no-stake) form of the
team's payment key, so it is spendable yet carries no stake. After enough
magazines the pool's largest UTxO shrinks and `prep-magazine.sh` (which funds
from the biggest UTxO) starts to fail — the ADA still exists, just fragmented
and parked. `reclaim-funds.sh` sweeps it back: each operator has their own flood
pool, keys are held in the internal key store, and we hold a key for every place
the ADA lands.

**Legend:** *defrag* = consolidate an address's many UTxOs into one; *sweep* =
move every UTxO to another address.

The reclaim tool is dry-run by default (builds + signs, prints txids, submits
nothing). It batches inputs so a badly fragmented address doesn't overflow the
max tx size, and `--loop` repeats rounds — waiting for each to confirm — until
the source is a single UTxO.

Dry run by default (builds+signs, prints txids, submits nothing). It batches
inputs (`--max-inputs`, default 200) so a badly fragmented address doesn't
overflow the max tx size, and `--loop` repeats rounds — waiting for each to
confirm — until the source is a single UTxO.

**`--site` re-gathers what's still at your pool; the parking sweep recovers what
escaped to the sink.** Two ways to handle parking, which commingles *both*
sites' drained lanes:

- `--parking --to SITE|ADDR` sweeps the **entire** sink to one destination (you
  own where it lands; if both operators run it, it's a race for the whole
  balance).
- `--rebalance` brings both pools to a **fair balance** in one shot, sourcing
  from parking and — when parking can't cover it because one
  pool already holds the surplus — a partial **pool-to-pool transfer** from the
  richer pool (its own `-flood.skey`). Default policy `equalize` makes both
  pools end at the same balance (`target=(Cc+Cd+P)/2`, each receives
  `target − its_balance`), so the unrecoverable fee loss is shared 50/50.
  `--policy proportional` instead splits parking by each pool's shortfall from
  `--funded` (default 500000 ADA). Parking UTxOs are assigned **largest-first to
  the side with the greater remaining need**, so the split's overshoot is
  bounded by the smallest UTxO — an arbitrary-order greedy skews badly when
  parking holds a few large UTxOs. Exact per-lane attribution isn't possible from
  a bare node (needs db-sync/an explorer and the original magazines), so this
  balance split is the practical fair option.

After a rebalance, run `--site` for each pool to consolidate it back to a
single spendable UTxO.

## Cursor / rate fidelity

The node writes a `<magazine>.cursor` sidecar (next-record byte offset) every N
injected txs (`transactions.tx_source_checkpoint_every`, default 1000) and
resumes from it on restart. This is for *rate fidelity*, not safety — replaying
disjoint-input txs is idempotent (a re-sent spent tx is rejected, never
double-spends). Delete the sidecar to replay a magazine from the start.

## Status

- `sizing.py`, `magazine.py` (garbage / pack / info / `--repeat`): working, tested.
- `make-magazine.sh` (T24 / T26): build + sign + pack verified against the live
  testnet node — both modes produce real signed txs (conflict variants share one
  input with distinct txids), and packed records are byte-exact with the signed
  CBOR.
- **`net-node` injection into a live Haskell node: WORKING** (verified
  2026-08-11) — an honest magazine tx was replayed through `net-node --tx-source`
  and **landed on chain**. The node must list net-node as a **trustable local
  root** (it won't pull from an unsolicited inbound peer); use
  `netnode-local-inject.toml` and follow the full recipe + gotchas in
  [`../local-node-injection.md`](../local-node-injection.md). Bead `9zq.4`.
- `prep-magazine.sh` (T31 fan-out): cardano-cli invocations mirror
  the internal key-store runbook but the full fan-out→chain flow has **not** been run
  end-to-end against the testnet yet — validate before a real run (bead `9zq.6`).
- `reclaim-funds.sh`: arg-parsing / mode validation exercised offline; the
  `--report` view and the build→sign→(submit) rounds mirror the same cardano-cli
  (dijkstra) calls as `make-magazine.sh`/`prep-magazine.sh` but have **not** been
  run against a live node yet. Do a `--report`, then a dry run, then `--submit`
  before trusting `--loop`.
