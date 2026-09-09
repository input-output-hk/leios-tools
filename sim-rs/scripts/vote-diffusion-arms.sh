#!/usr/bin/env bash
# Run the vote diffusion arms over one scenario and print the comparison.
#
# Arms can differ in delivery, votes cast and quorum under CPU/network load.
# Summary output only, no trace files, so a run costs minutes and megabytes
# rather than hours and gigabytes.
#
#   ./scripts/vote-diffusion-arms.sh [-t topology] [-s slots] [-p extra.yaml]...
#
# Arms are defined by the parameters/vote-*.yaml overlays.
# push-no-dedupe is deliberately absent: it does not terminate.

set -euo pipefail
cd "$(dirname "$0")/.."

TOPOLOGY="test_data/thousand.yaml"
SLOTS=300
EXTRA=(-p parameters/linear-with-tx-references.yaml -p parameters/turbo.yaml)
COMMITTEE="everyone"
SEED=0

while getopts "t:s:p:c:S:h" opt; do
  case "$opt" in
    t) TOPOLOGY="$OPTARG" ;;
    s) SLOTS="$OPTARG" ;;
    p) EXTRA+=(-p "$OPTARG") ;;
    c) COMMITTEE="$OPTARG" ;;
    S) SEED="$OPTARG" ;;
    h) sed -n '2,12p' "$0"; exit 0 ;;
    *) exit 1 ;;
  esac
done

# Always rebuild: a stale binary silently rejects newly added strategy values
# and produces an empty comparison rather than an error you would notice.
BIN="target/release/sim-cli"
cargo build --release

SCENARIO="$(mktemp -t vote-arms-scenario)"
trap 'rm -f "$SCENARIO"' EXIT
cat > "$SCENARIO" <<YAML
committee-selection-algorithm: "$COMMITTEE"
quorum-weight-fraction: 0.75
seed: $SEED
YAML

echo "topology=$TOPOLOGY slots=$SLOTS committee=$COMMITTEE seed=$SEED"
echo "binary=$("$BIN" --version)"
echo

# The final summary repeats the periodic one, so every figure is taken from
# the last occurrence of its line.  Bodies and total wire traffic are quoted
# separately: they are the two numbers the arms differ on, and the body count
# alone puts the arm that sends the most messages at the top of the table.
last_line() {
  echo "$out" | grep -F "$1" | tail -1 | sed 's/^ *//'
}

for arm in announce push push-late-dedupe push-echo; do
  out=$("$BIN" "$TOPOLOGY" -s "$SLOTS" "${EXTRA[@]}" \
        -p "$SCENARIO" -p "parameters/vote-$arm.yaml" 2>&1 |
        sed 's/\x1b\[[0-9;]*m//g')
  echo "== $arm =="
  last_line 'total votes were generated'
  last_line 'Vote body message(s) were sent'
  last_line 'Vote mini-protocol traffic sent'
  last_line 'Quorum at the stake-weighted median node'
  last_line 'vote bundle(s) reached 95% of nodes'
  echo
done
