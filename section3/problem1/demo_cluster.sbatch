#!/bin/bash
#SBATCH --job-name=hw3-docdemo
#SBATCH --nodes=3
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=4G
#SBATCH --time=00:20:00
#SBATCH --output=logs/docdemo_%j.log
#SBATCH --partition=debug
#
# Scripted version of the PDF's 1.5 demo on three nodes:
#   node[0] server, node[1] client 1, node[2] client 2.
# Transcripts land in demo/ so the README can quote them verbatim.
set -u
cd "$SLURM_SUBMIT_DIR/section3/problem1"
mkdir -p logs demo
module load python/3.12.5
PY=$HOME/hw3venv/bin/python
NODES=($(scontrol show hostnames "$SLURM_JOB_NODELIST"))
SRV=${NODES[0]}; C1=${NODES[1]}; C2=${NODES[2]}
PORT=${PORT:-50151}   # not 50051: another job's gRPC server may be on the same node
ADDR=$SRV:$PORT
echo "server=$SRV client1=$C1 client2=$C2"
run() { srun --overlap -N1 -n1 -w "$1" --quiet "${@:2}"; }

run $SRV $PY server.py 0.0.0.0:$PORT > demo/server.log 2>&1 &
sleep 4

# Client 1: create + open, wait for client 2 to subscribe, then edit twice.
(echo 'create report.txt "Hello World"'; sleep 1; echo "open report.txt"; sleep 8; echo 'edit report.txt 6 "Distributed "'; sleep 2; echo 'edit report.txt 0 "[v2] "'; sleep 3; echo "exit") \
  | run $C1 $PY client.py $ADDR client1 > demo/client1_transcript.txt 2>&1 &
C1PID=$!
sleep 4
# Client 2: open, subscribe, receive the two updates while idle, re-open, exit.
(echo "open report.txt"; sleep 1; echo "subscribe report.txt"; sleep 12; echo "open report.txt"; sleep 1; echo "exit") \
  | run $C2 $PY client.py $ADDR client2 > demo/client2_transcript.txt 2>&1 &
C2PID=$!
wait $C1PID $C2PID

echo "--- concurrent edits from node $C1 (two clients, same position) ---"
run $C1 $PY concurrent_demo.py $ADDR --clients 2 --doc concurrent2.txt > demo/concurrent_2clients.txt 2>&1
run $C2 $PY concurrent_demo.py $ADDR --clients 8 --doc concurrent8.txt > demo/concurrent_8clients.txt 2>&1
echo "--- error handling ---"
run $C2 $PY test_functional.py $ADDR > demo/functional_test.txt 2>&1

kill %1 2>/dev/null; sleep 1
for f in demo/client1_transcript.txt demo/client2_transcript.txt demo/concurrent_2clients.txt demo/concurrent_8clients.txt demo/functional_test.txt demo/server.log; do
  echo "=================== $f"; cat "$f"; done
