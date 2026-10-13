# Leios Simulation

This directory contains a Rust simulation of the Leios protocol. It produces a stream of events which can be used to visualize or analyze the behavior of Leios.

For more information about the simulation, see [./IMPLEMENTATION.md](./IMPLEMENTATION.md).

## Running the project

```sh
cargo run --release input_path [output_path] [-s slots] [--trace-node <node id>]

# for example...
cargo run --release ./test_data/realistic.yaml output/out.jsonl
```

The `input_path` is a YAML file which describes the network topology. Input files for predefined scenarios are in the `test_data` directory.

The default parameters for the simulation are defined in `data/simulation/config.default.yaml` in the root of this repository, which is symlinked to `parameters/config.default.yaml` here. To override parameters, pass `-p <path-to-parameters-file>` (you can pass this flag as many times as you'd like). Some predefined overrides are in the `parameters` directory.

While the simulation is running, it will log what's going on to the console. You can stop it at any time with ctrl+c, and when you do it will save the stream of events to `output_path`. To only simulate e.g. 50 slots, pass `-s 50`. Ordinary runs stopped with Ctrl-C exit successfully after saving their events and final statistics. If `--vote-traffic` is requested, interruption instead discards that capture and exits unsuccessfully because the traffic report requires a completed interval.

The simulation runs in virtual time and completes as fast as your machine allows.

To run the simulation on a more realistic network, use the pseudo-mainnet topology in `data/simulation/pseudo-mainnet/`:

```
cargo run --release \
  ../data/simulation/pseudo-mainnet/topology-v4-mainnet.yaml \
  output/mainnet.jsonl \
  -s 1000 \
  -p "./sim-cli/configs/mainnet.yaml"
```

NOTE: the `output/mainnet.jsonl` file can grow huge very quickly. Unless really needed, can be omitted.

## Engine and shard selection

The simulator supports two execution engines, selected via the `engine` parameter:

| Engine | Description | Deterministic | Attacker support |
|---|---|---|---|
| `actor` (default) | Tokio-based async actor system with virtual clock coordination | No | Yes |
| `sequential` | Discrete event simulation with strict timestamp ordering | Yes | No |

Both engines support **sharding**: partitioning nodes into independent groups that run in parallel. The sequential engine additionally uses rayon to parallelise simultaneous events across nodes within each timestep. Cross-shard communication uses conservative message blocking (CMB) based on minimum inter-shard latencies. Configure with:

- `shard-count` — number of shards (default: 1)
- `shard-strategy` — node assignment strategy:
  - `round-robin` — simple round-robin by node ID
  - `zero-latency-clusters` — keeps zero-latency-connected nodes together (recommended)
  - `geographic` — k-means clustering by geographic coordinates
  - `min-latency-clusters` — agglomerative clustering by link latency
  - `min-cut` — recursive bisection with Kernighan-Lin refinement

For fast runs, a convenience preset is provided:

```sh
cargo run --release topology.yaml output.jsonl -s 500 -p parameters/turbo.yaml
```

This uses the sequential engine with 6 shards and `zero-latency-clusters`, typically giving ~5x speedup over the default actor engine.

> [!NOTE]
> For instructions on running the simulation using Docker, please refer to the Docker Simulation section in the root README.md.

## Vote diffusion experiments

The [Linear Leios vote study](docs/vote-diffusion-study.md) compares
announce/request and push transport, crosses fanout with deduplication order,
and distinguishes everyone-votes load tests from the proposed stake-weighted
fixed-size committee. It documents the corrected measurements, reproducible
run matrix and limitations of the earlier results. The [focused findings](docs/vote-diffusion-followup-20260915/README.md)
cover BP protection within the fanout cap, control-message size sensitivity
and absolute traffic at individual BPs and relays.

## Network partitions

The simulator supports time-windowed network-layer partitions: a `partition-scenarios` parameter block cuts a set of directed edges at `start-time-s` and, if `stop-time-s` is set, heals them again. Scenarios are resolved against the topology at init (unknown node names are a hard error), and only edges that actually exist are cut — the true count is reported at runtime:

```
Network partition 'eu-na-split' activated: 53744 edge(s) cut.
Network partition 'eu-na-split' healed: 53744 edge(s) restored.
```

Partitions are kept in their own overlay file rather than in the experiment config, so the same config can run with and without the partition. Pass the overlay as the **last** `-p`.

A commented example covering all selector shapes (`set-to-set` with `both`/`from-to`/`to-from` directions, and `isolate`) is in [`parameters/partition-example.yaml`](./parameters/partition-example.yaml), written against the small test topology:

```sh
cargo run --release -- test_data/simple.yaml /tmp/partition-demo.jsonl \
    -s 120 -p parameters/partition-example.yaml
```

For continent-scale cuts on the pseudo-mainnet topologies, generate an overlay from the topology's country-code metadata (the `*.yaml.meta.json` sidecar) instead of listing nodes by hand:

```sh
python3 scripts/gen-partition.py \
    ../data/simulation/pseudo-mainnet/topology-v4-mainnet.yaml \
    --from EU --to NA --start 300 --stop 900 -o eu-na.yaml

cargo run --release \
  ../data/simulation/pseudo-mainnet/topology-v4-mainnet.yaml \
  -s 1000 \
  -p "./sim-cli/configs/mainnet.yaml" \
  -p eu-na.yaml
```

Omitting `stop-time-s` keeps the partition in place until the end of the simulation. Scenarios may overlap in time; each cut/heal is applied independently.

## Using traces in model comparisons

### Transaction diffusion

Assuming an output file `simplified.json`:

```sh
./txn_diffusion.sh simplified.json
```

This will output a ΔQ expression for use with the `delta_q` web tool corresponding to the probabilistic choice between all diffusion traces contained in the JSON file.
