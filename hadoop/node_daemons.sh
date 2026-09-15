#!/bin/bash
# Runs on EVERY node of the allocation, one instance per node, for the whole
# life of the job (launched with `srun ... &` from cluster.sbatch).
#
# Daemons run in the foreground on purpose: Slurm reaps anything left behind
# when an srun step exits, so `hdfs --daemon start` would be killed the moment
# this script returned.  Keeping them as children of a step that never returns
# is what keeps the cluster alive.
set -u
source "$(dirname "$0")/env.sh"
MASTER=$1
ME=$(hostname -s)
JOB=${SLURM_JOB_ID:-manual}
LOCAL=${HADOOP_LOCAL_ROOT:-/tmp/$USER}/hadoop-$JOB
mkdir -p "$LOCAL"/{logs,pids,tmp}
pids=()
cleanup() { kill "${pids[@]}" 2>/dev/null; wait; rm -rf "$LOCAL"; }
trap cleanup EXIT INT TERM

if [ "$ME" = "$MASTER" ]; then
  hdfs namenode -format -force -nonInteractive > "$LOCAL/logs/format.log" 2>&1
  hdfs namenode      > "$LOCAL/logs/namenode.out" 2>&1 & pids+=($!)
  yarn resourcemanager > "$LOCAL/logs/rm.out" 2>&1 & pids+=($!)
  mapred historyserver > "$LOCAL/logs/jhs.out" 2>&1 & pids+=($!)
  sleep 3
fi
hdfs datanode      > "$LOCAL/logs/datanode.out" 2>&1 & pids+=($!)
yarn nodemanager   > "$LOCAL/logs/nm.out" 2>&1 & pids+=($!)
echo "[$ME] daemons up: ${pids[*]}"
wait -n   # if any daemon dies, tear the node down so the failure is visible
echo "[$ME] a daemon exited; stopping node"
