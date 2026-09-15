#!/bin/bash
#SBATCH --job-name=hw3-hadoop
#SBATCH --nodes=4
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=80G
#SBATCH --time=04:00:00
#SBATCH --output=logs/hadoop_%j.log
#SBATCH --partition=debug
#
# Brings up a private Hadoop 3.3.6 (HDFS + YARN) cluster on the allocated
# nodes, runs WORKLOAD (a script, executed on the master node with the Hadoop
# environment loaded), then holds the cluster for HOLD_SECONDS so more jobs can
# be submitted with  srun --jobid=$SLURM_JOB_ID --overlap -w $MASTER ...
#
#   sbatch --export=ALL,WORKLOAD=path/to/script.sh hadoop/cluster.sbatch
#   sbatch --export=ALL,HOLD_SECONDS=7200 hadoop/cluster.sbatch     # just hold
set -u
cd "$SLURM_SUBMIT_DIR"
mkdir -p logs
source hadoop/env.sh
NODES=$(scontrol show hostnames "$SLURM_JOB_NODELIST" | tr '\n' ' ' | sed 's/ $//')
MASTER=${NODES%% *}
CONF=$SLURM_SUBMIT_DIR/hadoop/conf-$SLURM_JOB_ID
VCORES=${SLURM_CPUS_PER_TASK:-16}
MEM_MB=$(( ${SLURM_MEM_PER_NODE:-81920} - 8192 ))   # leave room for the daemons themselves

echo "job=$SLURM_JOB_ID master=$MASTER nodes='$NODES' vcores/node=$VCORES yarn-mem/node=${MEM_MB}MB"
bash hadoop/gen_config.sh "$CONF" "$MASTER" "$NODES" "$VCORES" "$MEM_MB"
ln -sfn "$CONF" hadoop/conf-live
export HADOOP_CONF_DIR=$CONF

# ---- start daemons on every node (one long-running step) ------------------
srun --nodes="$SLURM_JOB_NUM_NODES" --ntasks-per-node=1 --export=ALL,HADOOP_CONF_DIR="$CONF" \
     bash hadoop/node_daemons.sh "$MASTER" &
DAEMON_STEP=$!

# ---- wait until HDFS has left safe mode and YARN sees every NodeManager -----
want=$SLURM_JOB_NUM_NODES
for i in $(seq 1 60); do
  sleep 5
  live_dn=$(hdfs dfsadmin -report 2>/dev/null | grep -c '^Name:')
  live_nm=$(yarn node -list 2>/dev/null | grep -c RUNNING)
  echo "  waiting... datanodes=$live_dn/$want nodemanagers=$live_nm/$want"
  if [ "$live_dn" -ge "$want" ] && [ "$live_nm" -ge "$want" ]; then
    hdfs dfsadmin -safemode wait >/dev/null 2>&1; break
  fi
done
echo "=== cluster ready ==="
hdfs dfsadmin -report | grep -E 'Live datanodes|Configured Capacity' | head -2
yarn node -list 2>/dev/null | grep -E 'Total Nodes|RUNNING'
hdfs dfs -mkdir -p /user/$USER

# ---- workload -------------------------------------------------------------
if [ -n "${WORKLOAD:-}" ]; then
  echo "=== running workload: $WORKLOAD ==="
  bash "$WORKLOAD" 2>&1
  echo "=== workload finished (exit $?) ==="
fi

HOLD=${HOLD_SECONDS:-0}
if [ "$HOLD" -gt 0 ]; then
  echo "=== holding cluster for ${HOLD}s;  run:  srun --jobid=$SLURM_JOB_ID --overlap -N1 -w $MASTER bash -c 'source hadoop/env.sh; ...' ==="
  sleep "$HOLD"
fi

echo "=== shutting down ==="
kill $DAEMON_STEP 2>/dev/null
scancel --signal=TERM "$SLURM_JOB_ID.0" 2>/dev/null
sleep 5
rm -f hadoop/conf-live
echo done
