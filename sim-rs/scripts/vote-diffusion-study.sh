#!/usr/bin/env bash
# Matched Linear Leios vote study: everyone-votes load test plus the fixed-size,
# stake-weighted CIP #1250 committee; announce/request and both push dedupe orders.
# Usage: scripts/vote-diffusion-study.sh <study config.yaml> <new output dir> [seed ...]
# See docs/vote-diffusion-study.md for matrix controls and interpretation.
set -euo pipefail

here=$(cd "$(dirname "$0")/.." && pwd)
cfg=${1:?study config.yaml}
out=${2:?new output dir}
shift 2
seeds=("$@")
if [ ${#seeds[@]} -eq 0 ]; then seeds=(0); fi
read -r -a sizes <<< "${VOTE_STUDY_SIZES:-750 1500}"
read -r -a committees <<< "${VOTE_STUDY_COMMITTEES:-everyone top-stake-seats}"
read -r -a fanouts <<< "${VOTE_STUDY_FANOUTS:-all 22 16 8}"
slots=${VOTE_STUDY_SLOTS:-400}
dry_run=${VOTE_STUDY_DRY_RUN:-0}

# Validate the matrix before building or starting an expensive run.
for seed in "${seeds[@]}"; do [[ "$seed" =~ ^[0-9]+$ ]] || { echo "Invalid seed: $seed" >&2; exit 1; }; done
[[ "$slots" =~ ^[1-9][0-9]*$ ]] || { echo "Invalid slot count: $slots" >&2; exit 1; }
for size in "${sizes[@]}"; do
  case "$size" in 750|1500) ;; *) echo "Invalid size: $size" >&2; exit 1 ;; esac
done
for committee in "${committees[@]}"; do
  case "$committee" in everyone|top-stake-seats) ;; *) echo "Invalid committee: $committee" >&2; exit 1 ;; esac
done
for fanout in "${fanouts[@]}"; do
  [[ "$fanout" == all || "$fanout" =~ ^[1-9][0-9]*$ ]] || { echo "Invalid fanout: $fanout" >&2; exit 1; }
done
[[ "$dry_run" == 0 || "$dry_run" == 1 ]] || { echo "VOTE_STUDY_DRY_RUN must be 0 or 1" >&2; exit 1; }

# Separate output directories keep previous results and their inputs intact.
mkdir "$out"
cp "$cfg" "$out/study-config.yaml"
cp "$here/parameters/study-linear-tx-load.yaml" "$out/workload.yaml"
cp "$here/parameters/turbo.yaml" "$out/engine.yaml"
git -C "$here" rev-parse HEAD > "$out/revision.txt"
git -C "$here" diff HEAD -- . ../shared-rs > "$out/source.patch"
for size in "${sizes[@]}"; do
  if [ "$size" = 750 ]; then
    cp "$here/../data/simulation/pseudo-mainnet/topology-v2-cip.yaml" "$out/topology-750.yaml"
  else
    python3 - "$here/../data/simulation/pseudo-mainnet/topology-v2-1500.yaml" "$out/topology-1500.yaml" <<'PY'
import json, sys
with open(sys.argv[1]) as source:
    data = json.load(source)
for node in data['nodes'].values():
    node['cpu-core-count'] = 4
    for peer in node.get('producers', {}).values():
        peer['bandwidth-bytes-per-second'] = 1250000
with open(sys.argv[2], 'w') as dest:
    json.dump(data, dest)
PY
  fi
done

bin=$here/target/release/sim-cli
if [ "$dry_run" = 0 ]; then
  cargo build --release --locked --manifest-path "$here/Cargo.toml" --bin sim-cli >/dev/null
  "$bin" --version > "$out/binary.txt"
  python3 - "$bin" > "$out/binary.sha256" <<'PYHASH'
import hashlib, sys
with open(sys.argv[1], 'rb') as binary:
    digest = hashlib.sha256()
    for chunk in iter(lambda: binary.read(1024 * 1024), b''):
        digest.update(chunk)
    print(digest.hexdigest())
PYHASH
fi
printf 'run,seed,nodes,committee,transport,fanout,slots,status\n' > "$out/runs.csv"
failed=0

run_arm() {
  local transport=$1 fanout=$2
  local name="$size-$committee-$transport-f$fanout-s$seed"
  local overlay="$out/$name.yaml"
  local cap=$fanout
  [ "$cap" = all ] && cap=null
  cat > "$overlay" <<YAML
committee-selection-algorithm: "$committee"
committee-seat-count: 900
quorum-weight-fraction: 0.75
seed: $seed
vote-transport: "$transport"
vote-push-fanout: $cap
vote-transport-echo-to-source: false
YAML
  local status=planned
  if [ "$dry_run" = 0 ]; then
    status=passed
    if ! "$bin" "$out/topology-$size.yaml" -s "$slots" \
        -p "$out/study-config.yaml" -p "$out/workload.yaml" \
        -p "$out/engine.yaml" -p "$overlay" > "$out/$name.txt" 2>&1; then
      status=failed
      failed=1
    fi
  fi
  printf '%s,%s,%s,%s,%s,%s,%s,%s\n' "$name" "$seed" "$size" "$committee" "$transport" "$fanout" "$slots" "$status" >> "$out/runs.csv"
  echo "$name: $status"
}

for seed in "${seeds[@]}"; do
  for size in "${sizes[@]}"; do
    for committee in "${committees[@]}"; do
      run_arm announce-then-request all
      for fanout in "${fanouts[@]}"; do
        run_arm push "$fanout"
        run_arm push-late-dedupe "$fanout"
      done
    done
  done
done
exit "$failed"
