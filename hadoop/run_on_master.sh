#!/bin/bash
# Run a command on the master node of a held cluster job, from the login node.
#   bash hadoop/run_on_master.sh JOBID 'hdfs dfs -ls /'
JOBID=$1; shift
MASTER=$(scontrol show hostnames "$(squeue -h -j "$JOBID" -o %N)" | head -1)
srun --jobid="$JOBID" --overlap -N1 -n1 -w "$MASTER" --cpus-per-task=1 \
     bash -c "cd $HOME/HW3 && source hadoop/env.sh && $*"
