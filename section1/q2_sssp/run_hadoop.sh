#!/bin/bash
# SSSP on Hadoop Streaming: prep job -> iterate until the SSSP.UPDATED counter
# is 0 -> final formatting job.
#
#   bash run_hadoop.sh INPUT_LOCAL OUTPUT_LOCAL [--reducers R] [--maps M]
#                      [--mode frontier|full] [--tag NAME] [--keep]
#
# Requires the Hadoop environment (source ../../hadoop/env.sh).  Appends one
# line per iteration to results/iterations.csv and one per run to results/runs.csv.
set -euo pipefail
cd "$(dirname "$0")"
IN=$1; OUT=$2; shift 2
R=2; M=2; MODE=frontier; TAG=""; KEEP=0
while [ $# -gt 0 ]; do
  case $1 in
    --reducers) R=$2; shift 2;; --maps) M=$2; shift 2;; --mode) MODE=$2; shift 2;;
    --tag) TAG=$2; shift 2;; --keep) KEEP=1; shift;; *) echo "unknown $1"; exit 2;;
  esac
done
: "${STREAMING_JAR:?source hadoop/env.sh first}"
mkdir -p results logs
[ -f results/iterations.csv ] || echo "run,tag,mode,reducers,maps,stage,iter,secs,updated,map_output_records" > results/iterations.csv
RUN=sssp_$(date +%s)_$$
HD=/user/$USER/$RUN
LOG=logs/$RUN.log
[ -n "$TAG" ] || TAG=$(basename "$IN")

now() { date +%s.%N; }
counter() {  # counter NAME LOGFILE -> value ("" if absent); NAME may contain ( )
  local pat; pat=$(printf '%s' "$1" | sed 's/[()]/\\&/g')
  { grep -E "^\s*$pat=" "$2" || true; } | tail -1 | cut -d= -f2 | tr -d '[:space:]'
}
job() {  # job NAME ARGS...  -> runs a streaming job, returns wall seconds via $JOB_SECS
  local name=$1; shift
  local t0; t0=$(now)
  hadoop jar "$STREAMING_JAR" -D mapreduce.job.name="$RUN-$name" "$@" >> "$LOG" 2>&1 \
    || { echo "job $name failed, see $LOG" >&2; tail -30 "$LOG" >&2; exit 1; }
  JOB_SECS=$(echo "$(now) - $t0" | bc)
}

echo "[$RUN] input=$IN reducers=$R maps=$M mode=$MODE" | tee -a "$LOG"
T_START=$(now)

# ---- upload ---------------------------------------------------------------
hdfs dfs -mkdir -p "$HD"
hdfs dfs -put -f "$IN" "$HD/graph.txt"
BYTES=$(hdfs dfs -stat %b "$HD/graph.txt")
# old-API TextInputFormat: splitSize = max(minsize, min(bytes/job.maps, blocksize)); pin it to bytes/M
SPLIT=$(( (BYTES + M - 1) / M )); [ "$SPLIT" -lt 1024 ] && SPLIT=1024
T_UP=$(now)

# ---- prep: edge list -> vertex records ---------------------------------------
job prep -D mapreduce.job.reduces="$R" \
  -D mapreduce.job.maps="$M" \
  -D mapreduce.input.fileinputformat.split.minsize="$SPLIT" \
  -files bin/prep_map,bin/prep_reduce \
  -mapper prep_map -reducer prep_reduce \
  -input "$HD/graph.txt" -output "$HD/iter0"
echo "  prep      ${JOB_SECS}s"
echo "$RUN,$TAG,$MODE,$R,$M,prep,0,$JOB_SECS" >> results/iterations.csv

# ---- iterate ----------------------------------------------------------------
i=0
while true; do
  : > "$LOG.iter"
  t0=$(now)
  hadoop jar "$STREAMING_JAR" -D mapreduce.job.name="$RUN-iter$((i+1))" \
    -D mapreduce.job.reduces="$R" \
    -files bin/sssp_map,bin/sssp_combine,bin/sssp_reduce \
    -cmdenv SSSP_MODE="$MODE" \
    -mapper sssp_map -combiner sssp_combine -reducer sssp_reduce \
    -input "$HD/iter$i" -output "$HD/iter$((i+1))" > "$LOG.iter" 2>&1 \
    || { echo "iteration $((i+1)) failed" >&2; tail -30 "$LOG.iter" >&2; exit 1; }
  cat "$LOG.iter" >> "$LOG"
  secs=$(echo "$(now) - $t0" | bc)
  upd=$(counter UPDATED "$LOG.iter"); upd=${upd:-0}
  mapout=$(counter "Map output records" "$LOG.iter")
  i=$((i+1))
  echo "  iter $i    ${secs}s  UPDATED=$upd  map-output-records=$mapout"
  echo "$RUN,$TAG,$MODE,$R,$M,iter,$i,$secs,$upd,$mapout" >> results/iterations.csv
  [ "$KEEP" = 1 ] || hdfs dfs -rm -r -skipTrash "$HD/iter$((i-1))" > /dev/null
  [ "$upd" = 0 ] && break
done
T_ITER=$(now)

# ---- final: numeric sort + "node dist" formatting -----------------------------
job final -D mapreduce.job.reduces=1 \
  -D stream.num.map.output.key.fields=1 \
  -D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator \
  -D mapreduce.partition.keycomparator.options=-n \
  -D "mapreduce.output.textoutputformat.separator= " \
  -files bin/final_map,bin/final_reduce \
  -mapper final_map -reducer final_reduce \
  -input "$HD/iter$i" -output "$HD/final"
echo "  final     ${JOB_SECS}s"
echo "$RUN,$TAG,$MODE,$R,$M,final,0,$JOB_SECS" >> results/iterations.csv

hdfs dfs -cat "$HD/final/part-*" > "$OUT"
T_END=$(now)
[ "$KEEP" = 1 ] || hdfs dfs -rm -r -skipTrash "$HD" > /dev/null

TOTAL=$(echo "$T_END - $T_START" | bc); UPLOAD=$(echo "$T_UP - $T_START" | bc)
echo "[$RUN] iterations=$i total=${TOTAL}s (upload ${UPLOAD}s)  output -> $OUT"
[ -f results/runs.csv ] || echo "run,tag,mode,reducers,maps,iterations,upload_s,total_s" > results/runs.csv
echo "$RUN,$TAG,$MODE,$R,$M,$i,$UPLOAD,$TOTAL" >> results/runs.csv
