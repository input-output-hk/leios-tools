# txflow

Declarative transaction-flow experiments. An experiment describes the UTxO graph to
create (which transaction spends which output) with no tx ids. `txflow` expands it into a
symbolic graph IR, derives the conflicts, and builds every transaction offline against a
chain context. Design: [`specs/002-tx-flow-experiments`](../../specs/002-tx-flow-experiments/).

This first cut covers `expand` and `materialise`. Submitting (`fund`, `replay` over
N2N/N2C), schedules, results and import are not implemented yet; `schedule` and `expect`
in a spec are parsed and carried, not interpreted.

## Example: from definition to signed transactions

An experiment goes through three steps, each more concrete than the last.

### 1. Definition

[`examples/conflicting-chains.yaml`](examples/conflicting-chains.yaml) is written by
hand. It gives the shape only (no tx ids, keys or network), so it can be rerun anywhere:

```yaml
patterns:
  - id: chains
    use: conflicting_chains
    with: { length: "{{ length }}", conflict: per_step }   # length: 3
```

This asks for two chains a1 → a2 → a3 and b1 → b2 → b3, where each aᵢ conflicts with bᵢ.

### 2. Expand

```sh
cd net-rs
cargo run -p txflow -- expand txflow/examples/conflicting-chains.yaml
```

The pattern becomes a graph of named transactions. Names are `<pattern id>/<name>`
(`chains/a2`), and `.N` picks an output (`chains/a1.0` is a1's output 0). Sources are
placeholders: `chains/wa` and `chains/wb` fund the two chains, and `chains/c1..c3` are the
shared inputs, each "an output worth 2 ADA" with no tx id yet.

```yaml
chains/a2:
  inputs: [chains/a1.0, chains/c2]   # a1's change output, plus the shared c2
  outputs: [{ to: alice, lovelace: change }]
```

Conflicts and orphans are then worked out from the inputs; nothing declares them:

```yaml
derived:
  topo_order: [chains/a1, chains/b1, chains/a2, chains/b2, chains/a3, chains/b3]
  conflict_sets:
    chains/c1: [chains/a1, chains/b1]
    chains/c2: [chains/a2, chains/b2]
    chains/c3: [chains/a3, chains/b3]
  orphan_on:
    chains/a2: [chains/c1]
    chains/a3: [chains/c1, chains/c2]
    ...
```

With `conflict: root` only `c1` is shared and the later transactions appear only under
`orphan_on`. `count: 10` gives ten independent copies named `chains_0/…` to `chains_9/…`
(any written `count`, even 1, names copies this way, so scaling it keeps names stable).

### 3. Materialise

```sh
cargo run -p txflow -- materialise txflow/examples/conflicting-chains.yaml \
  --context txflow/examples/demo-context.yaml -o /tmp/manifest.json
```

The graph is bound to one network (protocol parameters, funding key, offered UTxOs), and
every transaction is built and signed offline. A funding transaction turns each
placeholder into a real output, and the experiment transactions spend those:

| # | tx | spends | tx id |
|---|----|--------|-------|
| 0 | `fund/0` | the offered UTxO; creates one output per source | `12b1aaab…` |
| 1 | `chains/a1` | `chains/c1` = `12b1aaab…#0`, `chains/wa` = `12b1aaab…#3` | `b1787e7e…` |
| 2 | `chains/b1` | `chains/c1` = `12b1aaab…#0`, `chains/wb` = `12b1aaab…#4` | `6075515c…` |
| 3 | `chains/a2` | `chains/a1.0` = `b1787e7e…#0`, `chains/c2` = `12b1aaab…#1` | `ca515927…` |
| … | | | |

`a1` and `b1` both spend `12b1aaab…#0`, so the double spend is real. The manifest lists
every transaction in release order with its CBOR, fee and resolved inputs. Each one
carries CIP-20 metadata (label 674): `["txflow:<IR hash prefix>", "<run id>#<index>"]`.

The demo context uses a publicly known dummy key and a fake funding UTxO, so its
transactions are well formed but no node will accept them. For a real network, write a
context with the protocol parameters (`pparams_file` accepts `cardano-cli query
protocol-parameters` output), your payment `.skey`, and the UTxOs you offer; `txflow`
uses as many of them, in order, as the experiment needs.

## Patterns

| Pattern | Arguments (default) | Shape |
|---|---|---|
| `independent` | `n` (1), `outs` (1), `lovelace` (scales with `outs`) | `n` unrelated txs |
| `chain` | `length` (2), `outs` (1), `lovelace` (scales with `length`) | `t1 → … → tN` via change |
| `fan_out` | `width` (2), `lovelace` per branch | `root` with `width` outputs, each spent by `c<i>`; the last branch also gets the leftover |
| `fan_in` | `width` (2), `lovelace` | `width` leaves merged by `merge` |
| `conflict_set` | `k` (2), `lovelace` | `x1..xk` all spend `s` |
| `diamond` | `lovelace` per branch | `root` splits into `left` and `right` (which also gets the leftover), `merge` joins them |
| `conflicting_chains` | `length` (3), `lovelace`, `conflict_lovelace`, `conflict` (`per_step`/`root`) | `a1..aN` vs `b1..bN` |

Every pattern also takes `wallet`, required when the spec declares more than one. Arguments accept
`{{ expr }}` over `params` with `+ - * /` and parentheses. Explicit `sources:` and
`txs:` can be mixed with patterns.

## Limits of this cut

- Plain ADA payments to enterprise addresses; no scripts, tokens or datums yet. Offered
  funding UTxOs must hold ADA only: their token contents are not known offline.
- Limits, so a typo fails fast instead of exhausting memory: tx-count arguments at
  100,000, inputs or outputs per tx at 1,000, an experiment at 5,000,000 graph elements
  (sources, txs, inputs, outputs), and 2,000,000 ancestor relations (each tx's contested
  ancestors, which `orphan_on` is built from; they grow with chain length times the
  conflicts upstream, so long `per_step` conflicting chains hit it first).
- All wallets bind to one run key derived from the funding key, IR hash and seed.
  `txflow run-key <spec> --context <ctx> -o run.skey` writes it as a `cardano-cli`
  signing key (owner-only permissions on Unix, never overwriting a file), so leftover outputs at
  the run address can be spent with other tools.
- Built as Conway transactions (Pallas 1.4.0). Whether the Dijkstra-era devnets accept
  them is the open spike in `research.md` D2.
- The min-UTxO check uses an upper bound on the output size, so it is slightly
  conservative.
- Each transaction is checked for balance, fee, maximum size and min-UTxO while it is
  built; full phase-1 validation with `pallas-validate` (FR-015) is not wired in yet.
