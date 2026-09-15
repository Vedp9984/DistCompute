#!/bin/bash
# Correctness of the MapReduce weather analytics:
#   1. tests/*.txt (HW2 hand-made cases) vs stored expected output
#   2. generated datasets vs weather_seq (HW2 sequential program), several
#      mapper/reducer counts, both the one-stage and two-stage designs
#   3. one generated dataset vs the independent Python oracle from HW2
# Usage: bash verify.sh [--hadoop]   (--hadoop runs run_hadoop.sh instead of run_local.sh)
set -uo pipefail
cd "$(dirname "$0")"
HW2="../../q8_from hw2"
RUNNER=run_local.sh; [ "${1:-}" = "--hadoop" ] && RUNNER=run_hadoop.sh
make -s; (cd "$HW2" && make -s bin/weather_seq bin/gen_weather >/dev/null 2>&1 || make -s "bin/weather_seq" "bin/gen_weather")
SEQ="$HW2/bin/weather_seq"; GEN="$HW2/bin/gen_weather"
W=$(mktemp -d "$PWD/.verify.XXXXXX"); trap 'rm -rf "$W"' EXIT
pass=0; fail=0
run() { bash $RUNNER "$@" 2>"$W/log"; }
check() {  # label input expected [runner args]
  local label=$1 in=$2 exp=$3; shift 3
  if run "$in" "$W/got" "$@" && diff -q "$W/got" "$exp" >/dev/null; then pass=$((pass+1)); printf '  ok    %-52s %s\n' "$label" "$*"
  else fail=$((fail+1)); printf '  FAIL  %-52s %s\n' "$label" "$*"; diff "$W/got" "$exp" | head -6; tail -3 "$W/log"; fi
}
echo "== hand-made cases vs expected =="
for t in tests/*.txt; do
  n=$(basename $t .txt)
  check "$n" "$t" "tests/$n.expected" --maps 3 --reducers 2
  check "$n (1 stage)" "$t" "tests/$n.expected" --maps 2 --reducers 1 --stages 1
done
echo "== generated vs weather_seq =="
gen() { # label N S K span [runner args...]
  local label=$1 N=$2 S=$3 K=$4 span=$5; shift 5
  "$GEN" $N $S $K --seed 2024 --span $span --output "$W/in.txt" >/dev/null
  "$SEQ" "$W/in.txt" > "$W/exp"
  check "$label N=$N S=$S K=$K" "$W/in.txt" "$W/exp" "$@"
}
gen prime     200003 300  10 86400 --maps 7 --reducers 3
gen prime     200003 300  10 86400 --maps 1 --reducers 1 --stages 1
gen fewstn    100000 3    5  3600  --maps 4 --reducers 2
gen manystn   100000 4000 25 86400 --maps 4 --reducers 4
gen onebucket 50000  50   10 60    --maps 3 --reducers 2
gen tiny      7      5    3  600   --maps 4 --reducers 2
gen single    1      5    3  600   --maps 4 --reducers 2
gen small     1000000 500 10 86400 --maps 8 --reducers 4
echo "== generated vs independent Python oracle =="
"$GEN" 100003 200 10 --seed 7 --span 86400 --output "$W/in.txt" >/dev/null
python3 "$HW2/scripts/oracle.py" "$W/in.txt" > "$W/exp"
check "oracle N=100003" "$W/in.txt" "$W/exp" --maps 5 --reducers 2
echo "passed=$pass failed=$fail"; [ $fail = 0 ]
