#!/bin/bash
#SBATCH --job-name=q8-weather
#SBATCH --nodes=4
#SBATCH --ntasks-per-node=2
#SBATCH --cpus-per-task=2
#SBATCH --mem-per-cpu=4G
#SBATCH --time=06:00:00
#SBATCH --output=results/q8_%j.log
#SBATCH --error=results/q8_%j.err
#SBATCH --partition=debug

# Q8: build, generate datasets, verify correctness, then benchmark P = 1,2,4,8.
#
# Datasets are generated here rather than uploaded -- they run to several GB and
# are a pure function of the seed, so regenerating is faster than copying.
module load hpcx-2.7.0/hpcx-ompi 2>/dev/null || true

# Placement flags for the timed runs.  --bind-to core stops two ranks sharing a
# core; --map-by node spreads them one-per-node round-robin so the P=2 case
# crosses the network like every larger P does, making the comparison uniform.
BENCH_MPIFLAGS="--map-by node --bind-to core"

cd "$SLURM_SUBMIT_DIR" || exit 1
mkdir -p results

# Build into a per-job directory.  Two jobs submitted into the same checkout --
# easily done by accident -- would otherwise race: each starts with `make clean`
# (rm -rf bin), so one job deletes the binaries the other is midway through
# executing.  That shows up as an isolated, unreproducible test failure in one
# job and a bogus "build failed" in the other, with nothing in either log to
# explain it.  A private BIN per job makes concurrent submissions harmless.
export BIN="bin.$SLURM_JOB_ID"
trap 'rm -rf "$SLURM_SUBMIT_DIR/$BIN"' EXIT

echo "job $SLURM_JOB_ID  nodes=$SLURM_NNODES  tasks=$SLURM_NTASKS"
echo "nodelist: $SLURM_NODELIST"

echo "=== node topology ==="
lscpu | grep -E '^CPU\(s\):|^Thread|^Core|^Socket' || true
scontrol show job "$SLURM_JOB_ID" | grep -E 'NumNodes|NumCPUs|CPUs/Task' || true

# Placement is the thing that silently ruined the first run: two ranks sharing a
# physical core makes every speed-up number meaningless.  Print the binding so
# the log proves one rank per core rather than leaving it to trust.
echo "=== rank placement (one line per rank) ==="
mpirun $BENCH_MPIFLAGS -np "$SLURM_NTASKS" --report-bindings hostname 2>&1 | head -24

make BIN="$BIN" || { echo "build failed"; exit 1; }

echo "=== dataset generation ==="
bash scripts/gen_data.sh

# Phases are selectable.  Run as one job they are sequential, and a slow
# correctness sweep starves the benchmark of wall-clock -- which is exactly what
# happened: 6 hours went entirely into verification and the benchmark, the part
# the report actually needs, never started.  Submit them as two jobs instead:
#     sbatch --export=ALL,PHASE=bench  slurm/q8_job.sh
#     sbatch --export=ALL,PHASE=verify slurm/q8_job.sh
PHASE=${PHASE:-all}

if [ "$PHASE" = all ] || [ "$PHASE" = verify ]; then
    echo "=== correctness ==="
    bash scripts/verify.sh || echo "VERIFICATION FAILED"
fi

if [ "$PHASE" = all ] || [ "$PHASE" = bench ]; then
    echo "=== benchmark ==="
    MPIFLAGS="$BENCH_MPIFLAGS" bash scripts/benchmark.sh
fi

echo "done"
