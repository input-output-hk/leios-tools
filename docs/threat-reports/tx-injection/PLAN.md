# TX-injection pen-test family — transaction flooding via pre-signed magazines

**Threat coverage:** T25 *"Submit invalid transactions"*, T24 *"Submit duplicate
transactions"*, T26 *"Submit conflicting transactions"*, T31 *"Honest demand
exceeds SPO storage capabilities"*. Related: T16/T20 (censorship-by-displacement
as a *consequence* of flooding the mempool).

**What the family does:** an adversary that is **not a block producer** — just an
accepted peer — injects transactions into the network and relies on the honest
diffusion layer to gossip them. The only trust a net-rs peer needs to diffuse txs
is a matching `network_magic` in the handshake; there is no producer/stake check,
no authentication, and (in the net-rs local cluster) no ledger validation. The family
spans **garbage** txs (cheap, no keys) through **valid, fee-paying** txs (real
UTxOs, real cost), differing only in *what bytes fill the magazine*.

The unifying mechanism is a **magazine**: a pre-built file of opaque transaction
blobs that a node reads and re-broadcasts ("reads and shoots bytes"). The node
never parses tx internals, never holds signing keys, and never builds or signs —
all construction happens offline. This keeps the node a dumb, fast replayer and
keeps the treasury key off the attacking fleet.

---

## Two target environments

The same magazine mechanism runs against two very different targets, and *"does a
tx need to be signed?"* has opposite answers in each.

| | **net-rs (local cluster)** | **net-rs + testnet** |
|---|---|---|
| Nodes | `net-node` processes spawned by `net-cluster` | Real Haskell Leios nodes (a team-operated testnet relay) |
| Ledger | none — no UTxO model | real UTxO ledger, real fees, epoch snapshots |
| Tx validation | fake `tokio::sleep` (`net-node/src/mempool.rs`), then admit | full Cardano ledger rules |
| "Valid" means | nothing — any parseable CBOR is admitted | signed, funded, fee-paying, non-conflicting |
| Assets required | none | the offline treasury payment key + funded UTxOs (10M ADA), per-node keys |
| Garbage txs | **diffuse** (validation is a sleep) | **rejected at first honest node** (tests the reject path) |

net-rs is wire-compatible with real Cardano nodes (`net-cli handshake
backbone.cardano.iog.io:3001` negotiates against mainnet), so `net-cli submit
a testnet relay` genuinely pushes txs onto the real testnet. That
compatibility is what makes the testnet reachable from our tooling.

**Legend:** *magazine* = pre-built file of opaque signed/garbage tx blobs;
*net-rs (local cluster)* = local `net-node`/`net-cluster` with fake validation
(**not** the `sim-rs` simulator — these attacks run on the net-rs stack only);
*net-rs + testnet* = net-rs connected to the live testnet with a real ledger;
*network_magic* = the public chain identifier exchanged in the N2N handshake
(the only gate on tx diffusion).

---

## The magazine is threat-agnostic — one mechanism, four threats

The node does not know or care what is in the magazine, so the *same*
`--tx-source` feature covers the whole roadmap just by changing magazine contents:

| Magazine contents | Threat | Environment | Notes |
|---|---|---|---|
| Random CBOR blobs | **T25** invalid | net-rs local (diffuses) / testnet (tests rejection) | No keys. MVP. |
| 1 signed tx, repeated | **T24** duplicate | testnet | Tests dedup; cheapest real-tx case. |
| 2+ signed txs, same input | **T26** conflicting | testnet | Deliberate double-spend; tests conflict rejection. |
| N signed txs, disjoint inputs | **T31** load | testnet | The sustained flood; needs fan-out + fee budget. |

**Legend:** *disjoint inputs* = each tx spends a different fanned-out UTxO so no
two txs conflict; *dedup* = a node dropping a tx whose id it has already seen.

Code-wise this is a small variant of the existing generator: the honest generator
does `pool.push(make_fake_tx(...))` on a Poisson tick (`net-node/src/mempool.rs`);
the magazine source does `pool.push(magazine.next())` on the same tick. The whole
diffusion path downstream is unchanged.

---

## Architecture — node byte-shooter behind a `TxSource` seam

The node side is one abstraction with two implementations, built in order:

```
trait TxSource { fn next_batch(&mut self, n: usize) -> Vec<Bytes>; }
  FileTxSource    // now  — mmap/stream a local magazine file, in-memory cursor
  RemoteTxSource  // later — pull chunks (~1000) from the cluster manager
```

- **`net-node --tx-source <file>` + `FileTxSource` (D1, first):** each node owns a
  local magazine. Dead simple, self-contained, runs even if the cluster manager is
  down, parallel with no central bottleneck. Each node's magazine holds a
  **disjoint** set of inputs (partitioned at prep time), so nodes never conflict.
- **Cluster-manager supplier + `RemoteTxSource` (D2, later):** the cluster manager
  owns a **single** magazine and a **single** cursor and doles out chunks on
  request. Enables dynamic load-balancing (a harder-flooding node drains more) and
  live refill. Same magazine format; only the node-side source swaps. Build the
  seam now so D2 is a drop-in, not a rewrite.

| | D1 file-per-node | D2 central supplier |
|---|---|---|
| Simplicity | ✅ self-contained | ⚠️ adds a pull protocol |
| Coupling | none | node depends on supplier |
| Load balancing | ❌ static file sizing | ✅ dynamic |
| Live refill | ❌ regenerate files | ✅ append to central magazine |
| Single-node / outsider (no cluster) | ✅ works | ❌ needs supplier |

### Keys stay off the nodes

In both designs the node replays opaque bytes and never signs. The 10M-ADA
The treasury key (and per-node keys) lives **only** in the offline prep tool and, for
D2, the central supplier. A compromised/leaked attacking node then leaks nothing
spendable — directly honoring the team's "treat skeys as secrets" rule.
This is why we do **not** hand nodes raw UTxOs to build+sign themselves.

### Idempotent replay → the cursor is for *rate fidelity*, not safety

If every magazine tx spends a **disjoint** input, replaying an already-spent tx is
**harmless**: the ledger accepts it once and rejects every later copy (input
already consumed). A crash-and-replay-from-start therefore risks no double-spend
and no corruption — it only wastes sends. Consequences:

- **Do not build spent-tx write-back.** It guards a loss (abandoned unspent value)
  we accept (10M ADA, easily topped up), and adds a failure mode.
- **Do keep a cheap periodic offset cursor.** Its purpose is *rate fidelity*: a
  restarted node that re-sends spent txs lands them as rejected no-ops, so the
  *measured* flood rate overstates the *effective* rate and skews the attack math.
  Persist the file offset every K txs; on crash you re-send ≤K dupes — negligible
  dilution. With chained magazines (C>1) the cursor is per-lane (resume at the
  right link, else the rest of the chain orphans).

**The only** case where a spent input is *dangerous* is two txs sharing an input —
which happens only by deliberate construction (that is T26) or by re-fanning-out
the same source twice. Partition fan-out outputs across nodes and it never occurs.

---

## Magazine topology — fan-out width `W` and chain depth `C`

A magazine is built by **fanning out** one funded source UTxO into many outputs,
then pre-signing txs against them:

- **`W` = fan-out width** = number of independent source UTxOs (lanes). Lanes are
  mutually conflict-free.
- **`C` = chain depth** = pre-signed txs chained per lane, each 1-in-1-out tx
  spending the previous tx's output. `C=1` is pure fan-out; `C>1` lets one source
  UTxO seed a chain of `C` txs.
- Total pre-signed txs in a magazine = `W × C`.

`C` is the lever that turns "thousands of UTxOs" into "hundreds of lanes." But
**keep `C` modest on the real testnet**: we are deliberately overflowing mempools,
so if our own parent tx is evicted, every child below it in the chain orphans.
Prefer **`C ≈ 10–50` with wider `W`** over deep chains. (The exact safe `C` under
our own overflow is the one number worth validating empirically before committing
magazine sizes — see beads.)

---

## Sizing — from the throughput target to UTxO counts

### Operating point (Release-Candidate throughput target)

Sizing is driven by the Leios Release-Candidate throughput objective
(<https://leios.cardano-scaling.org/docs/roadmap/#release-candidate>): a modest
**5× today's throughput ≈ 22.5 TxkB/s** data rate, with L-timing 3·1+4+7 = 14 s,
giving an ideal certified-EB rate of `0.95^14 / 20 ≈ 0.0244 /s` (one EB every
~41 s) and ~922.5 kB of txs per EB (`S_EB-tx ≤ 1 MB`). We must be able to
**saturate and exceed** that operating point for a flood to be relevant.

Converting the 22.5 TxkB/s target to a tx rate under two tx-size profiles:

| Profile | Tx size | Rate `R` = 22.5 kB/s ÷ size | Txs per EB (×41 s) |
|---|---|---|---|
| **Mean** | 800 B | **28.1 tx/s** | 1,154 |
| **Tiny** (1-in-1-out) | 288 B | **78.1 tx/s** | 3,203 |

Both rows reproduce the reference 1154 / ~3200 txs-per-EB. **These rates are
trivial to *emit*** — the difficulty of a valid-tx flood is UTxO supply and fee
burn, not emission. The modest RC target is good news: a short prep pass yields a
meaningful test.

**Legend:** *TxkB/s* = transaction-kilobytes per second; *EB* = endorser block;
*L-timing* = the Leios stage durations summing to 14 s; *1-in-1-out* = the
smallest useful tx shape (one input, one output).

### The sizing formula

```
UTxOs needed (lanes W) = k · R · T / C
total pre-signed txs   = k · R · T
```

- `R` = target rate (28.1 or 78.1 tx/s) · `T` = duration (s)
- `k` = overflow factor — inject **above** target to stress rather than match. The
  net-rs mempool is a 10k-tx FIFO (evict-oldest); `k ≈ 2–3` overflows it and starts
  evicting honest txs (the censorship-by-displacement effect).
- `C` = chain depth (lanes = total ÷ C).

### UTxO counts (at target rate, `k=1`)

Pure fan-out (`C=1`):

| Duration | Mean 800B (28.1/s) | Tiny 288B (78.1/s) |
|---|---|---|
| 1 min | 1,686 | 4,686 |
| 10 min | 16,860 | 46,860 |
| 60 min | 101,160 | 281,160 |

Chained (Tiny profile) → source UTxOs / lanes only:

| Duration | C=1 | C=20 | C=50 |
|---|---|---|---|
| 1 min | 4,686 | 234 | 94 |
| 10 min | 46,860 | 2,343 | 937 |
| 60 min | 281,160 | 14,058 | 5,623 |

Multiply every cell by the overflow factor `k`, and divide by the number of
attacking nodes (each node owns a disjoint slice).

### Fee budget (the real meter)

Est. min fee ≈ `44 lovelace/byte · size + 155,381` ≈ **0.19 ADA (800B)** /
**0.17 ADA (288B)** per tx. *(Praos params — confirm against `cardano-cli dijkstra
query protocol-parameters` for the Dijkstra era.)* Chaining does not help; every
tx pays. Total fee = total txs × fee:

| Run | Total txs | Fee cost |
|---|---|---|
| Mean, 10 min | 16,860 | ~3,200 ADA |
| Mean, 60 min | 101,160 | ~19,200 ADA |
| Tiny, 10 min | 46,860 | ~8,000 ADA |
| Tiny, 60 min | 281,160 | ~47,800 ADA |

**10M-ADA runway at target rate (continuous):** ~**8.7 days** (Tiny) /
~**21 days** (Mean); at `k=3` overflow, ~2.9 / ~7 days. Fees are a non-issue for
any realistic test window. Min-UTxO lock is trivial: each lane holds ~1 ADA
(recoverable), so even 100k lanes locks ~1% of treasury — and chaining cuts lanes
by `C×`.

### Two reference test points (acceptance targets)

- **Smoke** — Mean, `k=1`, 1 min, single node, `C=10`: **~170 UTxOs, 1,686 txs,
  ~320 ADA.** Trivial prep; first end-to-end target.
- **Stress** — Tiny, `k=3`, 10 min, 10-node cluster, `C=20`: **~700 UTxOs/node
  (~7k lanes total), ~140k txs, ~24k ADA fees.** Well inside 10M.

---

## Per-node funding

Flooding needs **funded addresses (UTxOs)**, not stake or block production. Give
each attacking net-node its **own payment key + own UTxO set** (via `add-pool.sh
--stake`, which funds a fresh base address per pool), so N nodes fan out and flood
in parallel with no input contention. The pools/stake matter for the *other*
attack families and the <25% stake math, not for tx
flooding itself — but `add-pool.sh` conveniently mints a funded key per node.
Generalizing this to a fleet is the `9wc` stake-pool allocator.

---

## Gotchas (bank these; not blockers)

1. **TTL / validity window.** cardano-cli sets `invalid_hereafter`; a magazine is
   *perishable*. Build with a generous far-future TTL or treat magazines as
   use-soon artifacts, else pre-signed txs expire on the shelf.
2. **Fan-out parent must confirm first.** Child txs reference the fan-out tx's
   outputs. Build them immediately (the parent txid is known pre-submission), but
   they are not *accepted* until the parent lands (~a slot or two). Prep = submit
   fan-out → wait one confirmation → release the magazine.
3. **D2 batch size.** Hand out chunks (~1000), not one tx per round-trip, or the
   pull protocol becomes the bottleneck.
4. **Chain-depth vs eviction.** Deep chains are fragile under our own overflow.
   Validate a safe `C` empirically before sizing large magazines.
5. **Garbage on the testnet does not diffuse.** A real node drops it at the first hop;
   the T25 testnet experiment measures the *reject* path, not propagation.

---

## Work breakdown (beads)

Tracked under the epic **`tx-injection` — transaction flooding via pre-signed
magazines**. Run `bd ready` for status.

| Chunk | Threat / role | Depends on |
|---|---|---|
| Node `--tx-source` file replayer + `TxSource` seam + rate cursor | foundation | — |
| Magazine format + offline fan-out/prep tool + `k·R·T/C` calculator | foundation | node source |
| T25 garbage flood (net-rs local) — MVP + BT `tx-flood` action | T25 | node source |
| T24 duplicate + T26 conflicting — small real magazines (testnet) | T24, T26 | node source, prep tool |
| Chain-depth-vs-eviction empirical validation (pick safe `C`) | sizing | node source, prep tool |
| T31 sustained valid flood (testnet) — stress target | T31 | node source, prep tool, chain-depth |
| `RemoteTxSource` + cluster-manager supplier (D2) | scale-out | node source, prep tool |

---

## Status summary

| Chunk | design | node impl | prep tool | verified |
|---|---|---|---|---|
| Node `--tx-source` + `FileTxSource` | ✅ | ✅ (`net-node/src/tx_source.rs`) | — | ✅ unit tests |
| Magazine format + prep tool | ✅ | — | ✅ garbage/sizing/pack/`--repeat` | ✅ |
| T25 garbage (net-rs local) MVP | ✅ (this doc) | 📋 | ✅ (`magazine.py garbage`) | 📋 |
| T24 / T26 real magazines | ✅ (this doc) | ✅ (shared `tx_source`) | ✅ (`make-magazine.sh`) | ⏳ build+sign vs live node ✅; injection 📋 |
| Chain-depth validation | ✅ (this doc) | 📋 | 📋 | 📋 |
| T31 sustained flood | ✅ (this doc) | ✅ (shared `tx_source`) | ⏳ (`prep-magazine.sh`, untested live) | 📋 |
| D2 RemoteTxSource + supplier | ✅ (this doc) | 📋 | 📋 | 📋 |

Legend: ✅ done · ⏳ in progress · 📋 planned.

### `make-magazine.sh` (T24/T26 builder) — verified 2026-08-10

Built + signed real testnet txs against the live Leios node (`~752k ADA` at
the funded payment address, `txFeePerByte=44`, `txFeeFixed=155381`): `--mode duplicate`
emits one signed tx (packed `--repeat N`); `--mode conflict` emits N txs that
share one input with distinct txids (e.g. `d235dd9e…`, `7e15aa60…`, `e1875f39…`).
Packed magazine records are byte-exact with the signed CBOR. Not yet injected via
`net-node` replay — the node was mid-sync (~72%); the accept/reject observation
is bead `9zq.4`. (Note: this `cardano-cli` build has no `transaction view`
subcommand — ignore that suggestion where the docs mention it.)
