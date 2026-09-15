#!/bin/bash
# Weather analytics on Hadoop Streaming.
#
#   bash run_hadoop.sh INPUT_LOCAL_OR_HDFS OUTPUT_LOCAL [--maps M] [--reducers R]
#                      [--stages 1|2] [--tag NAME] [--no-combiner] [--keep]
#
# INPUT may be a local file (uploaded to HDFS first; upload time is reported
# separately) or an hdfs:// path already in HDFS (for benchmarks, so the upload
# is paid once per dataset).
#
# Stage 1: wx_mapper (in-mapper combining) -> wx_reducer as combiner -> R x wx_reducer
# Stage 2: identity mapper -> 1 x wx_final  (--stages 1: mapper -> 1 x wx_final)
#
# Timings are appended to results/hadoop_runs.csv, one line per run.
set -euo pipefail
cd "$(dirname "$0")"
IN=$1; OUT=$2; shift 2
M=4; R=2; STAGES=2; TAG=""; COMB=1; KEEP=0
while [ $# -gt 0 ]; do
  case $1 in
    --maps) M=$2; shift 2;; --reducers) R=$2; shift 2;; --stages) STAGES=$2; shift 2;;
    --tag) TAG=$2; shift 2;; --no-combiner) COMB=0; shift;; --keep) KEEP=1; shift;;
    *) echo "unknown $1"; exit 2;;
  esac
done
: "${STREAMING_JAR:?source hadoop/env.sh first}"
mkdir -p results logs
RUN=wx_$(date +%s)_$$
HD=/user/$USER/$RUN
LOG=logs/$RUN.log
[ -n "$TAG" ] || TAG=$(basename "$IN")
now() { date +%s.%N; }
counter() {  # counter NAME LOGFILE -> value ("" if absent); NAME may contain ( )
  local pat; pat=$(printf '%s' "$1" | sed 's/[()]/\\&/g')
  { grep -E "^\s*$pat=" "$2" || true; } | tail -1 | cut -d= -f2 | tr -d '[:space:]'
}

T0=$(now)
hdfs dfs -mkdir -p "$HD"
case "$IN" in
  hdfs://*|/user/*) HIN=$IN ;;
  *) hdfs dfs -put -f "$IN" "$HD/input.txt"; HIN=$HD/input.txt ;;
esac
T_UP=$(now)
BYTES=$(hdfs dfs -stat %b "$HIN")
# Streaming uses the old-API TextInputFormat: splitSize = max(minsize, min(bytes/job.maps, blocksize)).
# Setting job.maps=M *and* minsize=ceil(bytes/M) pins the split size to bytes/M, so
# we get M mappers whether M is above or below the number of HDFS blocks.
SPLIT=$(( (BYTES + M - 1) / M )); [ "$SPLIT" -lt 65536 ] && SPLIT=65536

echo "[$RUN] input=$IN ($BYTES bytes) maps=$M reducers=$R stages=$STAGES combiner=$COMB" | tee -a "$LOG"

# ---- stage 1 --------------------------------------------------------------
COMB_ARGS=(); [ "$COMB" = 1 ] && COMB_ARGS=(-combiner wx_reducer)
if [ "$STAGES" = 2 ]; then
  t0=$(now)
  hadoop jar "$STREAMING_JAR" -D mapreduce.job.name="$RUN-stage1" \
    -D mapreduce.job.reduces="$R" \
    -D mapreduce.job.maps="$M" \
    -D mapreduce.input.fileinputformat.split.minsize="$SPLIT" \
    -files bin/wx_mapper,bin/wx_reducer \
    -mapper wx_mapper "${COMB_ARGS[@]}" -reducer wx_reducer \
    -input "$HIN" -output "$HD/stage1" > "$LOG.s1" 2>&1 || { echo "stage 1 failed"; tail -30 "$LOG.s1"; exit 1; }
  cat "$LOG.s1" >> "$LOG"; S1=$(echo "$(now) - $t0" | bc)
  MAPS=$(counter "Launched map tasks" "$LOG.s1"); REDS=$(counter "Launched reduce tasks" "$LOG.s1")
  MAPOUT=$(counter "Map output records" "$LOG.s1"); SHUF=$(counter "Reduce shuffle bytes" "$LOG.s1")
  MAPMS=$(counter "Total time spent by all map tasks (ms)" "$LOG.s1"); REDMS=$(counter "Total time spent by all reduce tasks (ms)" "$LOG.s1")
  echo "  stage1  ${S1}s  maps=$MAPS reduces=$REDS map-output-records=$MAPOUT shuffle-bytes=$SHUF map-cpu-ms=$MAPMS reduce-cpu-ms=$REDMS"
  FIN_IN=$HD/stage1; FIN_MAPPER=cat; FIN_FILES=bin/wx_final
else
  S1=0; FIN_IN=$HIN; FIN_MAPPER=wx_mapper; FIN_FILES=bin/wx_mapper,bin/wx_final
fi

# ---- final stage ----------------------------------------------------------
t0=$(now)
hadoop jar "$STREAMING_JAR" -D mapreduce.job.name="$RUN-final" \
  -D mapreduce.job.reduces=1 \
  -D mapreduce.job.maps="$M" \
  -D mapreduce.input.fileinputformat.split.minsize="$SPLIT" \
  -files "$FIN_FILES" \
  -mapper "$FIN_MAPPER" -reducer wx_final \
  -input "$FIN_IN" -output "$HD/final" > "$LOG.s2" 2>&1 || { echo "final stage failed"; tail -30 "$LOG.s2"; exit 1; }
cat "$LOG.s2" >> "$LOG"; S2=$(echo "$(now) - $t0" | bc)
if [ "$STAGES" = 1 ]; then
  MAPS=$(counter "Launched map tasks" "$LOG.s2"); REDS=1
  MAPOUT=$(counter "Map output records" "$LOG.s2"); SHUF=$(counter "Reduce shuffle bytes" "$LOG.s2")
  MAPMS=$(counter "Total time spent by all map tasks (ms)" "$LOG.s2"); REDMS=$(counter "Total time spent by all reduce tasks (ms)" "$LOG.s2")
  echo "  single-stage  ${S2}s  maps=$MAPS map-output-records=$MAPOUT shuffle-bytes=$SHUF map-cpu-ms=$MAPMS reduce-cpu-ms=$REDMS"
else
  echo "  final   ${S2}s"
fi

# Streaming appends the key/value separator to a line that has no tab in it
# (the whole line is the key, the value is empty); strip it so the report is
# byte-identical to the sequential program's.
hdfs dfs -cat "$HD/final/part-*" | sed 's/\t$//' > "$OUT"
T_END=$(now)
[ "$KEEP" = 1 ] || hdfs dfs -rm -r -skipTrash "$HD" > /dev/null
TOTAL=$(echo "$T_END - $T_UP" | bc); UPLOAD=$(echo "$T_UP - $T0" | bc)
echo "[$RUN] job-time=${TOTAL}s (stage1 ${S1}s + final ${S2}s) upload=${UPLOAD}s -> $OUT"
[ -f results/hadoop_runs.csv ] || echo "run,tag,bytes,maps_req,reducers,stages,combiner,maps_launched,upload_s,stage1_s,final_s,total_s,map_output_records,shuffle_bytes,map_task_ms,reduce_task_ms" > results/hadoop_runs.csv
echo "$RUN,$TAG,$BYTES,$M,$R,$STAGES,$COMB,${MAPS:-},$UPLOAD,$S1,$S2,$TOTAL,${MAPOUT:-},${SHUF:-},${MAPMS:-},${REDMS:-}" >> results/hadoop_runs.csv
