#!/bin/bash
#SBATCH --job-name=hw3-grpc
#SBATCH --nodes=4
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=32G
#SBATCH --time=04:00:00
#SBATCH --output=logs/grpc_%j.log
#SBATCH --partition=debug
#
# gRPC streaming benchmark across 4 nodes:
#   node[0] coordinator   node[1..2] workers   node[3] streamer + query clients + dashboard
set -u
cd "$SLURM_SUBMIT_DIR/section2/q2_grpc"
mkdir -p logs data results
module load python/3.12.5
export PY=$HOME/hw3venv/bin/python
NODES=($(scontrol show hostnames "$SLURM_JOB_NODELIST"))
COORD=${NODES[0]}; W1=${NODES[1]}; W2=${NODES[2]}; CLI=${NODES[3]}
echo "job=$SLURM_JOB_ID coordinator=$COORD workers=$W1,$W2 client=$CLI"

HW2=../../q8_from\ hw2
(cd "$HW2" && make -s bin/gen_weather bin/weather_seq)
for sz in small medium; do
  case $sz in small) N=1000000; S=500; SPAN=86400;; medium) N=5000000; S=2000; SPAN=259200;; esac
  [ -f data/weather_$sz.txt ] || "$HW2/bin/gen_weather" $N $S 10 --seed 2024 --span $SPAN --output data/weather_$sz.txt >/dev/null
  [ -f data/weather_$sz.expected ] || "$HW2/bin/weather_seq" data/weather_$sz.txt > data/weather_$sz.expected
done
ls -l data/

B="$PY bench/bench.py --mode slurm --coord-node $COORD --worker-nodes $W1,$W2 --client-node $CLI --out results/${OUT:-grpc_bench.csv}"
SERIES=${SERIES:-ABCDE}
has() { case "$SERIES" in *$1*) return 0;; *) return 1;; esac; }
has A && echo "== A: worker count (batch 1000, rr, preloaded stream) =="
has A && $B --preload --dataset data/weather_small.txt  --expected data/weather_small.expected  --workers 1,2,4,8,12 --batch 1000 --dist rr --queries 0 --repeat 2 --tag A_workers
has A && $B --preload --dataset data/weather_medium.txt --expected data/weather_medium.expected --workers 1,2,4,8,12 --batch 1000 --dist rr --queries 0 --repeat 2 --tag A_workers
has A && echo "== A2: same, streamer parsing live (source-limited) =="
has A && $B --dataset data/weather_medium.txt --expected data/weather_medium.expected --workers 1,2,4,8 --batch 1000 --dist rr --queries 0 --tag A2_live
has B && echo "== B: batch size (W=4, rr, preloaded) =="
has B && $B --preload --dataset data/weather_small.txt  --expected data/weather_small.expected  --workers 4 --batch 1,10,100,1000,10000,100000 --dist rr --queries 0 --tag B_batch
has C && echo "== C: distribution strategy (preloaded) =="
has C && $B --preload --dataset data/weather_medium.txt --expected data/weather_medium.expected --workers 2,4,8 --batch 1000 --dist rr,station --queries 0 --tag C_dist
has D && echo "== D: concurrent query clients (W=4, batch 1000, preloaded) =="
has D && $B --preload --dataset data/weather_medium.txt --expected data/weather_medium.expected --workers 4 --batch 1000 --dist rr,station --queries 1,4,8,16 --tag D_queries
has E && echo "== E: throttled stream with 4 query clients (rate 200k rec/s) =="
has E && $B --dataset data/weather_small.txt  --expected data/weather_small.expected  --workers 4 --batch 1000 --dist rr --queries 4 --rate 200000 --tag E_rate
echo "== done =="
