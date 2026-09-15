#!/bin/bash
# Start W workers + coordinator locally, stream DATASET, query mid-stream, and
# diff the final report against weather_seq.
#   bash run_local.sh DATASET [W] [BATCH] [DIST] [RATE]
set -uo pipefail
cd "$(dirname "$0")"
PY=${PY:-$HOME/hw3venv/bin/python}
DATA=$1; W=${2:-3}; BATCH=${3:-1000}; DIST=${4:-rr}; RATE=${5:-0}
mkdir -p logs; pids=()
trap 'kill "${pids[@]}" 2>/dev/null; wait 2>/dev/null' EXIT
ws=""
for i in $(seq 0 $((W-1))); do
  p=$((50061+i)); $PY worker.py --port $p --id w$i > logs/worker_$i.log 2>&1 & pids+=($!); ws="$ws${ws:+,}localhost:$p"
done
sleep 1
$PY coordinator.py --port 50051 --workers "$ws" --dist "$DIST" > logs/coordinator.log 2>&1 & pids+=($!)
sleep 1.5
$PY streamer.py localhost:50051 "$DATA" --batch "$BATCH" --rate "$RATE" & SPID=$!
sleep 1.5
echo "--- mid-stream query ---"; $PY dashboard.py localhost:50051 --once | head -6
wait $SPID
$PY dashboard.py localhost:50051 --final --wait > logs/final_report.txt
"../../q8_from hw2/bin/weather_seq" "$DATA" > logs/seq_report.txt
if diff logs/final_report.txt logs/seq_report.txt; then echo "FINAL REPORT MATCHES weather_seq (W=$W batch=$BATCH dist=$DIST)"; else echo "MISMATCH"; exit 1; fi
