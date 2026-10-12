# Data Model: `txflow` file formats

Formats for the five artefacts in [`spec.md`](./spec.md): experiment spec, graph IR,
manifest, schedule and results. All human-edited files are YAML; machine-written files
are JSON or JSON Lines. Field names are `snake_case`. Every file carries `version: 0`
until the format is declared stable.

## 1. References and names

| Form | Meaning |
|---|---|
| `wa` | a **source**: an abstract funding output declared in `sources` |
| `a1.0` | output 0 of transaction `a1` |
| `chains/a1` | transaction `a1` emitted by pattern instance `chains` |
| `chains/a1.0` | output 0 of that transaction |

Transaction, source, wallet and pattern-instance names match `[a-z][a-z0-9_]*`. Pattern
instances namespace every name they emit with `<instance>/`. Labels, target names and the
experiment name are free strings. A reference that does not resolve is an error (FR-005).

## 2. Graph IR

The IR is the contract between expansion, import, materialisation and (later) sim-rs.

```yaml
version: 0
wallets: [alice]               # symbolic owners; bound to keys at materialise time
sources:
  <name>: { owner: <wallet>, lovelace: <u64> }
txs:
  <name>:
    inputs: [<ref>, ...]               # ordered; order is preserved in the built tx
    reference_inputs: [<ref>, ...]     # optional
    outputs:
      - to: <wallet> | script:<name>
        lovelace: <u64> | min | change # exactly one `change` per tx (default: last output)
        tokens: { <policy_sym>.<name>: <u64> }   # optional
        datum: { bytes: <u32> } | { inline: <cbor-hex> }   # optional
        pad_bytes: <u32>               # optional, inflates tx to a target size
    script: { validator: <name>, budget: { mem: <u64>, steps: <u64> } }   # optional
    template: { file: <path>.tx3, tx: <name>, args: { ... } }            # optional (story 5)
    validity: { from_slot_offset: <i64>, ttl_slots: <u64> }              # optional
    labels: [<string>, ...]
```

**Derived fields** (computed by `txflow`, written to the canonical IR, never hand-edited):

```yaml
derived:
  topo_order: [a1, b1, a2, b2, a3, b3]
  conflict_sets:            # sets of txs spending the same output, keyed by that output
    c1: [a1, b1]
    c2: [a2, b2]
  conflict_descendants:     # txs that become invalid if an ancestor loses its conflict
    a2: [c1]
  hash: blake2b256:<hex>    # over the canonical JSON of everything except `derived.hash`
```

**Rules**

- Inputs and outputs are values, not lists of UTxOs to discover: every input is a named
  reference, so the materialiser never selects coins (FR-011).
- An output referenced by two or more transactions forms a conflict set. Nothing else
  creates a conflict (FR-004).
- `change` absorbs the remainder after fees and explicit outputs; a negative remainder is
  a materialise-time error naming the tx.
- Canonical form: keys sorted, maps of txs and sources sorted by name, no YAML anchors,
  integers as integers. Two IRs with the same canonical form have the same hash (FR-006).

## 3. Experiment spec

```yaml
version: 0
experiment: <name>
seed: <u64>
params: { <name>: <number|string> }    # referenced as {{ name }} in pattern args
wallets:
  <name>: {}                            # MVP: all wallets bind to one key
patterns:
  - id: <instance-name>
    use: <pattern>
    count: <expr>                       # instances to emit, default 1
    with: { <arg>: <expr> }
txs: { ... }                            # optional explicit IR txs, same schema as §2
sources: { ... }                        # optional explicit sources
schedule: [ ... ]                       # see §5
expect: [ ... ]                         # see §7
```

`<expr>` is a number, a `{{ param }}` substitution, or simple arithmetic over params
(`{{ pairs * scale }}`). No conditionals or loops: anything richer belongs in a pattern.

### Pattern catalogue (MVP)

| Pattern | Emits | Arguments |
|---|---|---|
| `independent` | `count` unrelated single txs | `ins`, `outs`, `size` |
| `chain` | `t1 → … → tN` | `length`, `ins`, `outs`, `size` |
| `fan_out` | one tx with `width` outputs, each spent by a child | `width` |
| `fan_in` | `width` txs whose outputs one tx spends | `width` |
| `conflicting_chains` | chains `a` and `b` | `length`, `conflict: root \| per_step \| every_k`, `k` |
| `conflict_set` | `k` txs spending one source | `k` |
| `diamond` | fan-out, parallel chains, fan-in | `width`, `depth` |

After script support: `order_batch` (`users`, `orders_per_batch`, `batches`,
`validator`) and `contended_state` (`contenders`, `rounds`, `validator`).

`scale` is an ordinary param by convention; patterns multiply counts by it, never
lengths or widths unless the spec says so.

## 4. Manifest

Written by `txflow materialise`. JSON.

```json
{
  "version": 0,
  "run_id": "2026-10-12T14:03:11Z-7f3a",
  "ir_hash": "blake2b256:…",
  "seed": 42,
  "network": { "magic": 164, "era": "dijkstra", "pparams_hash": "blake2b256:…" },
  "wallets": { "alice": { "vkey_hash": "…" } },
  "funding": { "tx_id": "…", "cbor": "…", "sources": { "wa": "<txid>#0", "c1": "<txid>#2" } },
  "txs": {
    "a1": {
      "tx_id": "…", "cbor": "…", "size": 312, "fee": 171265,
      "inputs": [ { "ref": "wa", "utxo": "<txid>#0" }, { "ref": "c1", "utxo": "<txid>#2" } ],
      "conflict_set": "c1", "labels": ["side-a"], "pattern": "chains"
    }
  }
}
```

Transaction metadata (FR-013), label `1023` (proposed; must not collide with tx-firehose's
`1022` colour label): `{ "x": <experiment>, "r": <run_id>, "t": <symbolic name> }`.

## 5. Schedule

Part of the spec, or a separate file passed to `txflow replay`.

```yaml
schedule:
  - select: { label: side-a }         # or { names: [a1, a2] } or { pattern: chains }
    target: relay-tokyo               # name resolved through `targets`
    via: n2n                          # n2n | n2c
    at: 0ms                           # offset from replay start
    spacing: 0ms                      # between consecutive selected txs, in topo order
  - select: { label: side-b }
    target: relay-frankfurt
    via: n2n
    at: 0ms
targets:
  relay-tokyo: { host: "10.0.1.5:3001" }
  relay-frankfurt: { host: "10.0.2.5:3001" }
  node1-socket: { socket: "node1/node.socket" }
options:
  skip_descendants_of_rejected: false
```

A transaction matched by several selections is submitted by each. A transaction matched
by none is not submitted (a warning lists them). `trigger:` is reserved for event-based
release (out of scope for now).

## 6. Results

`results.jsonl`, one JSON object per line.

```json
{"t_ns": 0, "event": "run_start", "run_id": "…", "ir_hash": "…", "txflow": "0.1.0", "targets": {"relay-tokyo": {"node_version": "…"}}}
{"t_ns": 1203400, "event": "submitted", "tx": "chains/a1", "tx_id": "…", "target": "relay-tokyo", "via": "n2n"}
{"t_ns": 1450210, "event": "rejected", "tx": "chains/b1", "tx_id": "…", "target": "relay-frankfurt", "reason": "…"}
{"t_ns": 9800000000, "event": "on_chain", "tx": "chains/a1", "tx_id": "…", "slot": 1234, "block": "rb|eb", "block_hash": "…"}
{"t_ns": 60000000000, "event": "run_end", "expectations": [{"id": "one_per_conflict", "pass": true}]}
```

For N2N, "accepted" means the peer requested the body; the node's own mempool decision
is visible only through observation (`on_chain`) or node logs. This is recorded per event.

## 7. Expectations

```yaml
expect:
  - id: one_per_conflict
    each_conflict_set: { on_chain: exactly_one }
  - id: a_side_fast
    select: { label: side-a }
    on_chain_within: 40 slots
  - id: no_split_block
    each_conflict_set: { same_block: never }
```

## 8. Import (story 4)

`txflow import --db <postgres-url> --address <addr>... --slots <from>..<to> --fidelity shape|values|scripts`
writes a spec containing explicit `txs`, `sources`, a `schedule` reproducing relative
arrival times, and, where fitting succeeds, a commented-out equivalent `patterns` entry.
Role assignment: the script address becomes `script:<n>`; each distinct payment key
becomes `user_<n>` in first-seen order; a key that spends three or more script outputs in
one tx is labelled `batcher`.

## Worked example

The P1 experiment: a1→a2→a3 and b1→b2→b3, each aᵢ conflicting with bᵢ.

```yaml
version: 0
experiment: conflicting-chains
seed: 42
params: { chain_len: 3, scale: 1 }
wallets: { alice: {} }
patterns:
  - id: chains
    use: conflicting_chains
    count: "{{ scale }}"
    with: { length: "{{ chain_len }}", conflict: per_step }
schedule:
  - { select: { label: side-a }, target: node1, via: n2n, at: 0ms }
  - { select: { label: side-b }, target: node2, via: n2n, at: 0ms }
targets:
  node1: { host: "127.0.0.1:3001" }
  node2: { host: "127.0.0.1:3002" }
expect:
  - { id: one_per_conflict, each_conflict_set: { on_chain: exactly_one } }
```

It expands to:

```yaml
sources:
  chains/wa: { owner: alice, lovelace: 10000000 }
  chains/wb: { owner: alice, lovelace: 10000000 }
  chains/c1: { owner: alice, lovelace: 2000000 }
  chains/c2: { owner: alice, lovelace: 2000000 }
  chains/c3: { owner: alice, lovelace: 2000000 }
txs:
  chains/a1: { inputs: [chains/wa,   chains/c1], outputs: [{ to: alice, lovelace: change }], labels: [side-a] }
  chains/a2: { inputs: [chains/a1.0, chains/c2], outputs: [{ to: alice, lovelace: change }], labels: [side-a] }
  chains/a3: { inputs: [chains/a2.0, chains/c3], outputs: [{ to: alice, lovelace: change }], labels: [side-a] }
  chains/b1: { inputs: [chains/wb,   chains/c1], outputs: [{ to: alice, lovelace: change }], labels: [side-b] }
  chains/b2: { inputs: [chains/b1.0, chains/c2], outputs: [{ to: alice, lovelace: change }], labels: [side-b] }
  chains/b3: { inputs: [chains/b2.0, chains/c3], outputs: [{ to: alice, lovelace: change }], labels: [side-b] }
```

Conflict sets: `c1: [a1, b1]`, `c2: [a2, b2]`, `c3: [a3, b3]`. With `conflict: root`
only `c1` would exist, and a2/a3, b2/b3 would appear under `conflict_descendants`
instead.

Note the consequence of per-step conflicts: if a1 wins, b2 still conflicts with a2 via
`c2` *and* is orphaned by b1's loss. The results file reports both facts.
