#!/bin/bash
# Local emulation of the two-stage weather MapReduce without Hadoop.
#   bash run_local.sh INPUT [OUTPUT] [--maps M] [--reducers R] [--stages 1|2]
# The input is cut into M line-chunks (one per "mapper"), each chunk runs the
# mapper; `sort` plays the shuffle; the reducer merges; the final stage prints
# the report.  --stages 1 skips the intermediate reducer (mapper output goes
# straight to the single final reducer), the "one job, one reducer" design.
set -euo pipefail
cd "$(dirname "$0")"
IN=$1; shift; OUT=${1:-/dev/stdout}; [ $# -gt 0 ] && shift
M=4; R=2; STAGES=2
while [ $# -gt 0 ]; do case $1 in --maps) M=$2;; --reducers) R=$2;; --stages) STAGES=$2;; *) echo "unknown $1"; exit 2;; esac; shift 2; done
export LC_ALL=C
W=$(mktemp -d); trap 'rm -rf "$W"' EXIT
split -n l/$M -d -a 3 "$IN" "$W/chunk_"
for c in "$W"/chunk_*; do bin/wx_mapper < "$c" | sort -t$'\t' -k1,1 | bin/wx_reducer > "$c.map"; done   # combiner too
if [ "$STAGES" = 2 ]; then
  # partition by key hash across R reducers, each merges its share
  cat "$W"/chunk_*.map | awk -F'\t' -v R=$R -v W="$W" '{h=0; for(i=1;i<=length($1);i++) h=(h*31+ord[substr($1,i,1)])%R; print > (W "/part_" h)} BEGIN{for(n=0;n<256;n++) ord[sprintf("%c",n)]=n}'
  for p in "$W"/part_*; do sort -t$'\t' -k1,1 "$p" | bin/wx_reducer > "$p.red"; done
  cat "$W"/part_*.red | bin/wx_final > "$OUT"
else
  cat "$W"/chunk_*.map | sort -t$'\t' -k1,1 | bin/wx_final > "$OUT"
fi
