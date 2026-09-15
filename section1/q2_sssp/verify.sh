#!/bin/bash
# Correctness: local MR emulation vs Dijkstra oracle on hand-made and generated graphs.
set -uo pipefail
cd "$(dirname "$0")"
make -s
W=$(mktemp -d); trap 'rm -rf "$W"' EXIT
pass=0; fail=0
check() {  # name file
  for mode in frontier full; do
    bash run_local.sh "$2" "$W/got" --mode $mode --quiet 2>"$W/log"
    python3 oracle.py < "$2" > "$W/exp"
    if diff -q "$W/got" "$W/exp" >/dev/null; then pass=$((pass+1)); printf '  ok    %-40s %s\n' "$1" "$mode"
    else fail=$((fail+1)); printf '  FAIL  %-40s %s\n' "$1" "$mode"; diff "$W/got" "$W/exp" | head -5; fi
  done
}
echo "== hand-made =="
for f in tests/*.txt; do check "$(basename $f)" "$f"; done
echo "== generated =="
gen() { python3 gen_graph.py "$@" > "$W/g.txt"; check "V=$1 E=$2 ${*:3}" "$W/g.txt"; }
gen 10 20 --seed 1
gen 50 60 --seed 2 --shape chain
gen 100 300 --seed 3 --unreachable 0.3
gen 500 2000 --seed 4 --shape layered
gen 1000 1000 --seed 5 --shape star
gen 1000 5000 --seed 6
gen 10000 50000 --seed 7
gen 10000 20000 --seed 8 --shape layered --unreachable 0.1
echo "passed=$pass failed=$fail"
[ $fail = 0 ]
