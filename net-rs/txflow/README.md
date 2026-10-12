# txflow

Declarative transaction-flow experiments. An experiment describes the UTxO graph to
create (which transaction spends which output) with no tx ids. `txflow` expands it into a
symbolic graph IR, derives the conflicts, and builds every transaction offline against a
chain context. Design: [`specs/002-tx-flow-experiments`](../../specs/002-tx-flow-experiments/).

This first cut covers `expand` and `materialise`. Submitting (`fund`, `replay` over
N2N/N2C), schedules, results and import are not implemented yet; `schedule` and `expect`
in a spec are parsed and carried, not interpreted.

## Example: a1..a3 vs b1..b3

[`examples/conflicting-chains.yaml`](examples/conflicting-chains.yaml) asks for two
chains where each aᵢ conflicts with bᵢ:

```yaml
patterns:
  - id: chains
    use: conflicting_chains
    with: { length: "{{ length }}", conflict: per_step }
```

```sh
cd net-rs
cargo run -p txflow -- expand txflow/examples/conflicting-chains.yaml
```

prints the IR: five sources (`chains/wa`, `chains/wb` fund the chains, `chains/c1..c3`
are the shared inputs) and six transactions, plus what was derived from their inputs:

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
`orphan_on`. `count: 10` gives ten independent copies named `chains_0/…` to `chains_9/…` (any written
`count`, even 1, names copies this way, so scaling it keeps names stable).

```sh
cargo run -p txflow -- materialise txflow/examples/conflicting-chains.yaml \
  --context txflow/examples/demo-context.yaml -o /tmp/manifest.json
```

builds and signs one funding transaction and the six experiment transactions, with real
tx ids, fees and the inputs each one resolved to. `a1` and `b1` both spend the funding
output that stands for `chains/c1`, so the conflict is on the wire. Each transaction
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
