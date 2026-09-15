#!/bin/bash
# SSSP benchmark on the Hadoop cluster (WORKLOAD of hadoop/cluster.sbatch).
# Graphs are generated with fixed seeds; every result is checked against the
# Dijkstra oracle.  Appends to results/runs.csv and results/iterations.csv.
set -uo pipefail
cd "$(dirname "$0")"
module load python/3.12.5 2>/dev/null
mkdir -p results logs data
make -s || exit 1
[ -f results/correctness.txt ] && rm -f results/correctness.txt

gen() { # name V E extra...
  local name=$1 V=$2 E=$3; shift 3
  [ -f data/$name.txt ] || python3 gen_graph.py $V $E --seed 42 "$@" > data/$name.txt
  python3 oracle.py < data/$name.txt > data/$name.expected
}
run() { # name reducers maps mode
  local name=$1 r=$2 m=$3 mode=$4
  bash run_hadoop.sh data/$name.txt results/${name}_r${r}_m${m}_$mode.out --reducers $r --maps $m --mode $mode --tag $name 2>&1 | sed 's/^/  /'
  if diff -q results/${name}_r${r}_m${m}_$mode.out data/$name.expected >/dev/null; then
    echo "  [$name r=$r m=$m $mode] matches Dijkstra" | tee -a results/correctness.txt
  else echo "  [$name r=$r m=$m $mode] MISMATCH" | tee -a results/correctness.txt; fi
}

echo "== graphs =="
gen sample 4 4; cp tests/sample.txt data/sample.txt; python3 oracle.py < data/sample.txt > data/sample.expected
gen rnd1k    1000  5000
gen rnd10k   10000 50000
gen rnd10k_dense 10000 500000
gen rnd10k_unreach 10000 30000 --unreachable 0.2
gen layered10k 10000 40000 --shape layered
gen star10k  10000 10000 --shape star
gen chain40  40 39 --shape chain
ls -l data/*.txt | awk '{print "  " $5, $9}'

echo "== scaling with graph size (r=2, m=2, frontier) =="
for g in sample rnd1k rnd10k rnd10k_dense; do run $g 2 2 frontier; done
echo "== reducers / maps (rnd10k_dense) =="
for r in 1 4 8; do run rnd10k_dense $r 4 frontier; done
run rnd10k_dense 4 8 frontier
echo "== frontier vs full propagation =="
run rnd10k_dense 4 4 full
run rnd10k 2 2 full
echo "== graph shape: diameter drives the iteration count =="
for g in star10k layered10k rnd10k_unreach chain40; do run $g 2 2 frontier; done
echo "== done =="
cat results/correctness.txt
