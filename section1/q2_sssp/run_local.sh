#!/bin/bash
# Local emulation of the iterative SSSP MapReduce pipeline, without Hadoop:
#   mapper | sort | combiner | sort | reducer     repeated until UPDATED == 0
# `sort` plays the shuffle.  Counter lines the reducer writes to stderr are
# counted exactly the way the Hadoop driver reads the SSSP.UPDATED counter.
#
#   bash run_local.sh INPUT [OUTPUT] [--mode frontier|full] [--quiet]
set -euo pipefail
cd "$(dirname "$0")"
IN=$1; shift
OUT=${1:-/dev/stdout}; [ $# -gt 0 ] && shift
MODE=frontier; QUIET=0
while [ $# -gt 0 ]; do
  case $1 in --mode) MODE=$2; shift 2;; --quiet) QUIET=1; shift;; *) echo "unknown $1"; exit 2;; esac
done
export SSSP_MODE=$MODE LC_ALL=C
W=$(mktemp -d); trap 'rm -rf "$W"' EXIT

bin/prep_map < "$IN" | sort -t$'\t' -k1,1 | bin/prep_reduce > "$W/iter0"
i=0
while true; do
  bin/sssp_map < "$W/iter$i" | sort -t$'\t' -k1,1 | bin/sssp_combine | sort -t$'\t' -k1,1 \
    | bin/sssp_reduce > "$W/iter$((i+1))" 2> "$W/err"
  upd=$( { grep -o 'reporter:counter:SSSP,UPDATED,[0-9]*' "$W/err" || true; } | awk -F, '{s+=$3} END{print s+0}')
  i=$((i+1))
  [ $QUIET = 1 ] || echo "iteration $i: UPDATED=$upd" >&2
  [ "$upd" = 0 ] && break
done
bin/final_map < "$W/iter$i" | sort -n -k1,1 | bin/final_reduce | tr '\t' ' ' > "$OUT"
[ $QUIET = 1 ] || echo "converged after $i iterations" >&2
