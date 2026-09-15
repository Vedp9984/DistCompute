#!/bin/bash
#SBATCH --job-name=hw3-mpi
#SBATCH --nodes=4
#SBATCH --ntasks-per-node=4
#SBATCH --cpus-per-task=2
#SBATCH --mem=32G
#SBATCH --time=02:00:00
#SBATCH --output=logs/mpi_%j.log
#SBATCH --partition=debug
# HW2 MPI program on the same datasets and node type as the MapReduce runs.
cd "$SLURM_SUBMIT_DIR"
echo "job=$SLURM_JOB_ID nodes=$SLURM_JOB_NODELIST"
module load hpcx-2.7.0/hpcx-ompi
PHASE=mpi MPI_PROCS="1 2 4 8 16" MPI_REPS=3 bash section2/q1_mapreduce/bench_cluster.sh
