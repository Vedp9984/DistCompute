#!/bin/bash
# Q8 benchmark: four dataset sizes x P = 1,2,4,8, REPS runs each, plus the
# sequential program for reference.
#
# Writes results/q8_timings.csv:
#   size_label,N,bytes,P,rep,comm_s,compute_s,total_s
#
# "compute_s" is read + parse + local aggregation; "comm_s" is the collectives.
# Reporting the read separately in the log matters for the analysis: with
# parallel I/O it shrinks with P, which is the whole reason this scales.
set -uo pipefail
cd "$(dirname "$0")/.."

BIN=${BIN:-bin}
MPIRUN=${MPIRUN:-mpirun}
MPIFLAGS=${MPIFLAGS:---oversubscribe}
PROCS=${PROCS:-"1 2 4 8"}
REPS=${REPS:-3}
mkdir -p results
OUT=results/q8_timings.csv
echo "size_label,N,bytes,P,rep,comm_s,compute_s,total_s" > "$OUT"
SEQ=results/q8_sequential.csv
echo "size_label,N,bytes,rep,total_s" > "$SEQ"

for f in data/weather_*.txt; do
    [ -e "$f" ] || { echo "no datasets: run scripts/gen_data.sh first"; exit 1; }
done

for f in data/weather_small.txt data/weather_medium.txt \
         data/weather_large.txt data/weather_xlarge.txt; do
    [ -f "$f" ] || continue
    label=$(basename "$f" .txt); label=${label#weather_}
    N=$(head -1 "$f" | cut -d' ' -f1)
    bytes=$(stat -c%s "$f" 2>/dev/null || stat -f%z "$f")

    # Sequential reference for the same input.
    for rep in $(seq 1 "$REPS"); do
        line=$($BIN/weather_seq "$f" --time 2>&1 >/dev/null | grep '^seq ')
        tot=$(echo "$line" | sed -n 's/.*total=\([0-9.]*\).*/\1/p')
        echo "$label,$N,$bytes,$rep,$tot" >> "$SEQ"
        printf '  %-7s seq   rep=%s  total=%ss\n' "$label" "$rep" "$tot"
    done

    for P in $PROCS; do
        for rep in $(seq 1 "$REPS"); do
            line=$($MPIRUN $MPIFLAGS -np "$P" $BIN/weather_mpi "$f" --time </dev/null \
                     2>&1 >/dev/null | grep '^mpi ')
            rd=$(echo   "$line" | sed -n 's/.*read=\([0-9.]*\).*/\1/p')
            ps=$(echo   "$line" | sed -n 's/.*parse=\([0-9.]*\).*/\1/p')
            comm=$(echo "$line" | sed -n 's/.*reduce=\([0-9.]*\).*/\1/p')
            tot=$(echo  "$line" | sed -n 's/.*total=\([0-9.]*\).*/\1/p')
            comp=$(awk -v a="$rd" -v b="$ps" 'BEGIN{printf "%.6f", a+b}')
            echo "$label,$N,$bytes,$P,$rep,$comm,$comp,$tot" >> "$OUT"
            printf '  %-7s P=%s rep=%s  total=%ss (read %ss, parse %ss, reduce %ss)\n' \
                   "$label" "$P" "$rep" "$tot" "$rd" "$ps" "$comm"
        done
    done
done

echo
echo "raw timings -> $OUT (MPI), $SEQ (sequential)"
python3 ../common/plot.py --csv "$OUT" \
        --title "Q8 Weather Analytics" --outdir results --prefix q8 || \
    echo "(plotting skipped: python3/matplotlib unavailable)"
