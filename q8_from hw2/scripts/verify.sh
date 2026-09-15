#!/bin/bash
# Q8 correctness verification.
#
# Three layers, because two of them alone would not be enough:
#
#   1. tests/*.txt vs. their stored expected output -- catches regressions.
#   2. tests/*.txt vs. scripts/oracle.py, an independent Python implementation
#      written from the assignment text.  Two programs sharing a header can
#      share a misreading of the spec; the oracle does not.
#   3. Generated datasets: MPI at P = 1,2,4,8 vs. the sequential program.  This
#      is what actually exercises the byte-range splitting -- a record dropped
#      or double-counted at a rank boundary shows up here and nowhere else.
set -uo pipefail
cd "$(dirname "$0")/.."

BIN=${BIN:-bin}
MPIRUN=${MPIRUN:-mpirun}
# Cap every single run.  A deadlock in one case would otherwise sit until the
# scheduler kills the whole job -- which cost a 3-hour cluster allocation once,
# and produced a log that just stopped mid-suite with no indication why.
# `timeout` turns that into a visible TIMEOUT line and lets the suite continue.
CASE_TIMEOUT=${CASE_TIMEOUT:-120}
run_mpi() { timeout "$CASE_TIMEOUT" $MPIRUN $MPIFLAGS "$@" </dev/null; }
MPIFLAGS=${MPIFLAGS:---oversubscribe}
PROCS=${PROCS:-"1 2 4 8"}
fail=0
# Scratch files must live where EVERY rank can see them.  `mktemp -d` defaults to
# /tmp, which on a cluster is node-local: ranks on other nodes cannot open the
# generated input at all and produce empty output, so the case fails only at the
# process counts that span more than one node (P=4, P=8) and passes at P=1,2.
# That cost two full cluster allocations to track down.  Defaulting to the
# working directory keeps scratch on the shared filesystem the job was submitted
# from; override with VERIFY_TMPDIR if that is not shared.
TMP=$(mktemp -d "${VERIFY_TMPDIR:-$PWD}/.verify.XXXXXX")
trap 'rm -rf "$TMP"' EXIT

echo "== fixed cases: sequential vs. stored expected output =="
for t in tests/*.txt; do
    name=$(basename "$t" .txt)
    exp="tests/$name.expected"
    [ -f "$exp" ] || continue
    if diff -q <($BIN/weather_seq "$t") "$exp" >/dev/null; then
        printf '  PASS  %s\n' "$name"
    else
        printf '  FAIL  %s\n' "$name"; fail=1
    fi
done

echo "== fixed cases: sequential vs. independent Python oracle =="
for t in tests/*.txt; do
    name=$(basename "$t" .txt)
    if diff -q <($BIN/weather_seq "$t") <(python3 scripts/oracle.py "$t") >/dev/null; then
        printf '  PASS  %s\n' "$name"
    else
        printf '  FAIL  %s\n' "$name"; fail=1
    fi
done

echo "== fixed cases: MPI vs. sequential, P = $PROCS =="
for t in tests/*.txt; do
    name=$(basename "$t" .txt)
    $BIN/weather_seq "$t" > "$TMP/seq.txt"
    for P in $PROCS; do
        run_mpi -np "$P" $BIN/weather_mpi "$t" > "$TMP/mpi.txt" 2>/dev/null
        rc=$?
        if [ $rc -eq 124 ]; then
            printf '  TIMEOUT  %s P=%s (exceeded %ss -- likely a deadlock)\n' \
                   "$name" "$P" "$CASE_TIMEOUT"; fail=1
        elif diff -q "$TMP/seq.txt" "$TMP/mpi.txt" >/dev/null; then
            printf '  PASS  %s P=%s\n' "$name" "$P"
        else
            printf '  FAIL  %s P=%s (exit %s)\n' "$name" "$P" "$rc"; fail=1
            diff "$TMP/seq.txt" "$TMP/mpi.txt" | head -8
        fi
    done
done

echo "== generated datasets: MPI vs. sequential, P = $PROCS =="
# Sizes are deliberately awkward: N not divisible by P, a station count far
# below and far above P, and a span that puts many records in one 60 s bucket.
# 200003 is prime, so no byte-range boundary can land tidily.
run_case() {   # run_case N S K span label
    local N=$1 S=$2 K=$3 span=$4 label=$5
    local f="$TMP/gen_${label}.txt"
    $BIN/gen_weather "$N" "$S" "$K" --seed 2024 --span "$span" --output "$f"
    $BIN/weather_seq "$f" > "$TMP/seq.txt"
    if ! diff -q "$TMP/seq.txt" <(python3 scripts/oracle.py "$f") >/dev/null; then
        printf '  FAIL  %s (sequential disagrees with the Python oracle)\n' "$label"
        fail=1
    fi
    for P in $PROCS; do
        run_mpi -np "$P" $BIN/weather_mpi "$f" > "$TMP/mpi.txt" 2>/dev/null
        rc=$?
        if [ $rc -eq 124 ]; then
            printf '  TIMEOUT  %s N=%s S=%s P=%s (exceeded %ss -- likely a deadlock)\n' \
                   "$label" "$N" "$S" "$P" "$CASE_TIMEOUT"; fail=1
        elif diff -q "$TMP/seq.txt" "$TMP/mpi.txt" >/dev/null; then
            printf '  PASS  %s N=%s S=%s P=%s\n' "$label" "$N" "$S" "$P"
        else
            printf '  FAIL  %s N=%s S=%s P=%s (exit %s)\n' "$label" "$N" "$S" "$P" "$rc"
            diff "$TMP/seq.txt" "$TMP/mpi.txt" | head -12
            fail=1
        fi
    done
    rm -f "$f"
}

run_case 1000    50   5  3600   tidy
run_case 200003  97   10 86400  prime-N
run_case 50000   3    5  120    few-stations
run_case 50000   4000 20 600    many-stations
run_case 100000  200  10 60     one-bucket
run_case 7       3    5  600    tiny
run_case 1       1    1  60     single-record

echo
if [ "$fail" -eq 0 ]; then echo "ALL CHECKS PASSED"; else echo "FAILURES (see above)"; fi
exit $fail
