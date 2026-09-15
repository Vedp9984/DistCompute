#!/bin/bash
# Weather analytics: MapReduce benchmark + MPI re-run on the same nodes.
# Runs as the WORKLOAD of hadoop/cluster.sbatch (Hadoop env already loaded,
# cwd = ~/HW3).  Everything is appended to CSVs under results/; rerunning
# skips nothing, so start from a clean results/ for a fresh table.
#
#   SIZES="small medium large xlarge" MAPS="2 4 8 16 32" REDUCERS="1 2 4" REPS=2 bash section2/q1_mapreduce/bench_cluster.sh
set -uo pipefail
cd "$(dirname "$0")"
HW2="../../q8_from hw2"
# PHASE=mr  : everything except the MPI runs (needs the Hadoop cluster job)
# PHASE=mpi : only the MPI runs -- run as a SEPARATE Slurm job.  Inside the
#             Hadoop job mpirun cannot start: the daemon step holds every task
#             slot of the allocation, so mpirun's launcher waits forever.
PHASE=${PHASE:-all}
SIZES=${SIZES:-"small medium large xlarge"}
MAPS=${MAPS:-"2 4 8 16 32"}
REDUCERS=${REDUCERS:-"1 4"}
REPS=${REPS:-2}
MPI_PROCS=${MPI_PROCS:-"1 2 4 8 16"}
MPI_REPS=${MPI_REPS:-3}
MPIFLAGS=${MPIFLAGS:---map-by node --bind-to core}
mkdir -p results logs data
module load hpcx-2.7.0/hpcx-ompi 2>/dev/null || module load openmpi/4.1.5 2>/dev/null || true

echo "== build =="
[ "$PHASE" = mpi ] || make -s || exit 1
(cd "$HW2" && make -s) || exit 1
GEN="$HW2/bin/gen_weather"; SEQ="$HW2/bin/weather_seq"; MPI="$HW2/bin/weather_mpi"

declare -A N=([small]=1000000 [medium]=5000000 [large]=20000000 [xlarge]=50000000)
declare -A S=([small]=500 [medium]=2000 [large]=5000 [xlarge]=10000)
declare -A SPAN=([small]=86400 [medium]=259200 [large]=604800 [xlarge]=1209600)

echo "== datasets =="
for sz in $SIZES; do
  f=data/weather_$sz.txt
  [ -f "$f" ] || { echo "  generating $sz"; "$GEN" ${N[$sz]} ${S[$sz]} 10 --seed 2024 --span ${SPAN[$sz]} --output "$f" >/dev/null; }
  ls -l "$f" | awk '{print "  " $5, $9}'
done

if [ "$PHASE" != mpi ]; then
echo "== sequential baseline (T1) =="
[ -f results/seq_runs.csv ] || echo "size,bytes,rep,seq_s" > results/seq_runs.csv
for sz in $SIZES; do
  f=data/weather_$sz.txt; b=$(stat -c %s "$f")
  for rep in $(seq 1 $REPS); do
    t=$( { /usr/bin/time -f %e "$SEQ" "$f" --output results/seq_$sz.out; } 2>&1 | tail -1 )
    echo "  $sz seq rep$rep ${t}s"; echo "$sz,$b,$rep,$t" >> results/seq_runs.csv
  done
done

echo "== upload to HDFS (once per dataset) =="
[ -f results/hdfs_put.csv ] || echo "size,bytes,put_s" > results/hdfs_put.csv
for sz in $SIZES; do
  f=data/weather_$sz.txt; b=$(stat -c %s "$f")
  if ! hdfs dfs -test -e /user/$USER/data/weather_$sz.txt; then
    hdfs dfs -mkdir -p /user/$USER/data
    t0=$(date +%s.%N); hdfs dfs -put "$f" /user/$USER/data/weather_$sz.txt; t1=$(date +%s.%N)
    echo "  $sz put $(echo "$t1-$t0"|bc)s"; echo "$sz,$b,$(echo "$t1-$t0"|bc)" >> results/hdfs_put.csv
  fi
done

echo "== MapReduce runs =="
for sz in $SIZES; do
  for m in $MAPS; do
    for r in $REDUCERS; do
      for rep in $(seq 1 $REPS); do
        bash run_hadoop.sh /user/$USER/data/weather_$sz.txt results/mr_${sz}_m${m}_r${r}.out --maps $m --reducers $r --stages 2 --tag $sz 2>&1 | grep -E "^\[wx|stage1|final" | sed 's/^/  /'
        diff -q results/mr_${sz}_m${m}_r${r}.out results/seq_$sz.out >/dev/null && echo "  [$sz m=$m r=$r] matches sequential" || echo "  [$sz m=$m r=$r] MISMATCH"
      done
    done
  done
  # single-stage variant and no-combiner variant, one map setting, to show the design trade-off
  bash run_hadoop.sh /user/$USER/data/weather_$sz.txt results/mr_${sz}_1stage.out --maps 8 --reducers 1 --stages 1 --tag $sz 2>&1 | grep -E "^\[wx|single" | sed 's/^/  /'
  bash run_hadoop.sh /user/$USER/data/weather_$sz.txt results/mr_${sz}_nocomb.out --maps 8 --reducers 4 --no-combiner --tag $sz 2>&1 | grep -E "^\[wx|stage1" | sed 's/^/  /'
done

fi  # PHASE != mpi

if [ "$PHASE" = mr ]; then echo "== done (mr) =="; exit 0; fi
echo "== MPI runs (same nodes) =="
[ -f results/mpi_runs.csv ] || echo "size,bytes,P,rep,wall_s,read_s,parse_s,reduce_s,total_s" > results/mpi_runs.csv
for sz in $SIZES; do
  f=data/weather_$sz.txt; b=$(stat -c %s "$f")
  for P in $MPI_PROCS; do
    for rep in $(seq 1 $MPI_REPS); do
      out=$( { /usr/bin/time -f "WALL %e" mpirun $MPIFLAGS -np $P "$MPI" "$f" --output results/mpi_${sz}_p${P}.out --time </dev/null; } 2>&1 )
      wall=$(echo "$out" | awk '/^WALL/{print $2}')
      line=$(echo "$out" | grep '^mpi ' | tail -1)
      rd=$(echo "$line" | sed -n 's/.*read=\([0-9.]*\).*/\1/p'); ps=$(echo "$line" | sed -n 's/.*parse=\([0-9.]*\).*/\1/p')
      rdc=$(echo "$line" | sed -n 's/.*reduce=\([0-9.]*\).*/\1/p'); tot=$(echo "$line" | sed -n 's/.*total=\([0-9.]*\).*/\1/p')
      echo "  $sz P=$P rep$rep wall=${wall}s  $line"
      echo "$sz,$b,$P,$rep,$wall,$rd,$ps,$rdc,$tot" >> results/mpi_runs.csv
      diff -q results/mpi_${sz}_p${P}.out results/seq_$sz.out >/dev/null || echo "  MPI MISMATCH $sz P=$P"
    done
  done
done
echo "== done =="
