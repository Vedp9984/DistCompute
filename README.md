# Distributed Systems — Homework 3

| Section | Problem | Directory | Stack |
|---|---|---|---|
| 1 | Q2 — Single-Source Shortest Path, iterative MapReduce | `section1/q2_sssp/` | C++ Hadoop Streaming on Hadoop 3.3.6 / YARN |
| 2 Q1 | HW2 Q8 weather analytics as batch MapReduce, vs MPI | `section2/q1_mapreduce/` | C++ Hadoop Streaming; HW2 MPI re-run |
| 2 Q2 | HW2 Q8 weather analytics as a real-time stream | `section2/q2_grpc/` | Python gRPC: streamer, coordinator, N workers, CLI dashboard |
| 3 | Problem 1 — Collaborative document editing | `section3/problem1/` | Python gRPC, server-streaming subscriptions |

Each directory has its own README with setup, design, correctness
verification, experiments and results. Shared pieces:

| Path | Purpose |
|---|---|
| `hadoop/` | bringing up a private Hadoop 3.3.6 + YARN cluster inside a Slurm allocation on RCE (see `hadoop/README.md`) |
| `q8_from hw2/` | the HW2 Q8 code reused as generator (`gen_weather`), oracle (`weather_seq`, `scripts/oracle.py`) and MPI baseline (`weather_mpi`) |
| `tools/plotstyle.py` | one matplotlib style for every figure |
| `sync.sh` / `pull.sh` | push the repo to `~/HW3` on RCE / pull results, transcripts and logs back into `*/results/cluster/`, `section3/problem1/demo/`, `logs/cluster/` |
| `PLAN.md`, `context.md` | the implementation plan and the running progress log |

## Environment

* Local development: Python 3.12 venv with `grpcio grpcio-tools psutil matplotlib`
  (`~/hw3venv`), Hadoop 3.3.6 in pseudo-distributed mode
  (`~/apps/local_hadoop.sh start`), g++ 13.
* RCE cluster: `module load python/3.12.5 java/11.0.13 hpcx-2.7.0/hpcx-ompi`,
  the same venv, Hadoop 3.3.6 in `~/apps`, g++ 8.5. All cluster jobs are
  `sbatch` scripts (`hadoop/cluster.sbatch`, `section2/q2_grpc/grpc_bench.sbatch`,
  `section3/problem1/demo_cluster.sbatch`); every process runs on compute
  nodes, never on the login node.

## Reproducing everything

```bash
./sync.sh                                                         # code → cluster
ssh cs3401.20@rce.iiit.ac.in
cd ~/HW3
sbatch --export=ALL,WORKLOAD=hadoop/bench_all.sh --nodes=4 --cpus-per-task=16 --mem=100G --time=08:00:00 hadoop/cluster.sbatch
sbatch section2/q2_grpc/grpc_bench.sbatch
sbatch section3/problem1/demo_cluster.sbatch
exit
./pull.sh                                                          # results → results/cluster/
for d in section1/q2_sssp section2/q1_mapreduce section2/q2_grpc; do (cd $d && ~/hw3venv/bin/python plot.py); done
```

Datasets are never copied: the weather generator and the graph generator are
seeded and regenerate byte-identical inputs on the cluster.
