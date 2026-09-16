# context.md — HW3 progress log (read this first after a crash / context reset)

Last updated: 2026-09-16 10:45 IST — **ALL DELIVERABLES COMPLETE.**

## Scope (confirmed with user)
- Section 1 → **Q2 SSSP** iterative MapReduce, C++ Hadoop Streaming
- Section 2 → HW2 **Q8 weather analytics**: Q1 = Hadoop MapReduce (C++ streaming), Q2 = gRPC real-time streaming (Python)
- Section 3 → **Problem 1 Collaborative Document Editing** (gRPC, Python server.py/client.py)
- Full plan: `PLAN.md`. Assignment text: `hw3-final.pdf` (extracted text was in scratchpad; re-extract with `pdftotext -layout`).

## Cluster access (ADA / RCE)
- `ssh cs3401.20@rce.iiit.ac.in` — key-based (`~/.ssh/id_ed25519` installed on the cluster; no password needed).
- Slurm partition `debug`, nodes node01..07 (64 cores, 384 GB each). `/scratch` NOT on all nodes → Hadoop uses `/tmp/$USER/hadoop-$JOBID`.
- Modules used: `python/3.12.5`, `java/11.0.13` (JAVA_HOME=/usr/local/apps/jdk-11.0.13+8), `hpcx-2.7.0/hpcx-ompi` (MPI). g++ 8.5 (no static libstdc++ → do NOT use -static flags).
- Python venv with grpcio/grpcio-tools/psutil/matplotlib/numpy: `~/hw3venv` (cluster) and `~/hw3venv` (local laptop). Use `~/hw3venv/bin/python`.
- Hadoop 3.3.6 self-installed: cluster `~/apps/hadoop-3.3.6`; local `~/apps/hadoop-3.3.6`.
  (System hadoop 3.3.0 + shared HDFS exist at /usr/local/apps/hadoop-3.3.0 but YARN not running and wrong version → not used.)
- Repo on cluster: `~/HW3` (mirror of this dir). **`./sync.sh` pushes** (excludes bin/data/logs/results/demo). **`./pull.sh` fetches** results→`*/results/cluster/`, demo→`section3/problem1/demo/`, logs→`logs/cluster/`.
- Job logs on cluster: `~/HW3/logs/hadoop_<jobid>.log`, `grpc_<jobid>.log`, `docdemo_<jobid>.log`.

## Hadoop-on-Slurm (hadoop/)
- `hadoop/cluster.sbatch` brings up private HDFS+YARN on the allocated nodes (NN+RM on node[0], DN+NM on all; daemons run in foreground inside one `srun` step — Slurm kills detached daemons), waits for readiness, runs `$WORKLOAD` script on the master node with env loaded, optional `HOLD_SECONDS`.
  `sbatch --export=ALL,WORKLOAD=hadoop/bench_all.sh --nodes=4 --cpus-per-task=16 --mem=100G --time=08:00:00 hadoop/cluster.sbatch`
- `hadoop/env.sh` (source it), `gen_config.sh` (ports 21000-21099, disk-util threshold 99%), `node_daemons.sh`, `smoke.sh` (SSSP sample + weather handmade → prints SSSP_OK / WX_OK), `bench_all.sh`, `run_on_master.sh JOBID 'cmd'`.
- Local dev cluster: `source ~/apps/local_hadoop.sh start|stop` (pseudo-distributed, conf in ~/apps/conf-local). Each streaming job costs ~20 s overhead locally / ~18 s on cluster.
- Streaming gotchas learned: old-API TextInputFormat ignores split.maxsize → control mappers with `-D mapreduce.job.maps=M -D ...split.minsize=ceil(bytes/M)`. A reducer output line with no tab gets a trailing tab → SSSP final job sets output separator to " " with reducer emitting `node\tdist`; weather final report is stripped with `sed 's/\t$//'` in the driver.

## Section 1 Q2 — SSSP  (`section1/q2_sssp/`)  STATUS: code done + verified locally & on cluster; cluster benchmark RUNNING (job 93264)
- src: prep_map/prep_reduce (edge list → `node\tdist\tflag\tadj`), sssp_map/sssp_combine/sssp_reduce (iteration; counter `SSSP,UPDATED`; `SSSP_MODE=frontier|full`), final_map/final_reduce (numeric sort, "node dist"/INF).
- `run_local.sh` (pipe emulation), `run_hadoop.sh IN OUT [--reducers R --maps M --mode frontier|full --tag]` (loops until UPDATED==0; appends results/iterations.csv, results/runs.csv), `verify.sh` (28/28 pass vs Dijkstra `oracle.py`, both modes), `gen_graph.py` (random/chain/layered/star, --unreachable), `bench_cluster.sh`, tests/.
- Cluster: PDF sample verified on YARN (4 iterations, ~125 s total). rnd1k: 14 iterations.
- TODO: README.md (design, iteration table, plots from results/cluster/iterations.csv & runs.csv).

## Section 2 Q1 — Weather MapReduce (`section2/q1_mapreduce/`)  STATUS: code done + verified; cluster benchmark RUNNING (job 93264, after SSSP part)
- src: `weather_common.hpp` (copied from HW2), `wx_partial.hpp` (G / S:<id> / I:<bucket> / H lines, hex-float doubles), `wx_mapper` (in-mapper combining), `wx_reducer` (combiner + stage-1 reducer, idempotent merge), `wx_final` (1 reducer → exact HW2 report; K from forwarded H line or env WX_K).
- `run_local.sh`, `run_hadoop.sh IN OUT [--maps M --reducers R --stages 1|2 --no-combiner --tag]` (appends results/hadoop_runs.csv with counters), `verify.sh` (21/21 pass vs weather_seq + HW2 oracle.py), `bench_cluster.sh` (datasets small/medium/large/xlarge generated on cluster in data/, seq baseline, hdfs put timing, MR matrix maps∈{2,4,8,16,32}×reducers∈{1,4}×2 reps + 1-stage + no-combiner variants, then MPI P∈{1,2,4,8,16}×3 reps with `--map-by node --bind-to core` → results/{seq_runs,hdfs_put,hadoop_runs,mpi_runs}.csv).
- HW2 code lives in `q8_from hw2/` (space in the name! always quote). HW2 MPI numbers: `q8_from hw2/results/q8_tables.md`.
- TODO: README.md with design + MPI-vs-MR analysis + plots (plot.py to write).

## Section 2 Q2 — gRPC streaming (`section2/q2_grpc/`)  STATUS: code done + verified; cluster benchmark RUNNING (job 93269)
- `proto/weather.proto` (columnar RecordBatch; IngestService.StreamRecords client-stream; WorkerService.Process/GetPartial/GetInfo/Reset; QueryService.GetAnalytics). Regenerate stubs: `~/hw3venv/bin/python -m grpc_tools.protoc -Iproto --python_out=. --grpc_python_out=. proto/weather.proto`.
- `common.py` (Partial accumulator w/ Neumaier sums, merge, exact HW2 report_lines), `worker.py --port --id`, `coordinator.py --workers a:p,b:p --dist rr|station --port`, `streamer.py addr FILE --batch --rate --limit --preload --csv`, `dashboard.py addr [--once|--final --wait|--raw]`.
- `run_local.sh DATA W BATCH DIST` (end-to-end + diff vs weather_seq → all pass), `bench/query_load.py`, `bench/bench.py` (local|slurm mode matrix → results/grpc_bench.csv; ports 60051/60060+), `grpc_bench.sbatch` (4 nodes: coord / 2 worker nodes / client; series A workers, A2 live-parse, B batch, C dist, D queries, E rate).
- Findings so far: single streamer parsing live caps ~750k rec/s; W=1 ≈ 510–550k; with --preload W=4 ≈ 1.45M rec/s locally. Query latency ~1–25 ms.
- First cluster run (93266) had port clashes with the doc-demo server on node01:50051 → results saved as results/grpc_bench_run1_portclash.csv (ignore). Rerun = job 93269 (ports 60051+).
- TODO: README.md, plots (plot.py), architecture diagram (text), pull results.

## Section 3 P1 — Collaborative docs (`section3/problem1/`)  STATUS: DONE (code, tests, README written); cluster transcripts need re-capture
- `document.proto`, `server.py [addr]` (per-doc lock, subscriber queues, exits if bind fails), `client.py addr [name]`, `concurrent_demo.py addr --clients N`, `test_functional.py addr`, `demo_local.sh`, `demo_cluster.sbatch` (3 nodes, port 50151), README.md (has transcripts).
- Added subscription *snapshot* message (first DocumentUpdate has snapshot=true) → fixes observer race seen on cluster. Cluster demo job 93271 (3 nodes) PASSES all (2- and 8-client concurrent, functional test); transcripts pulled into demo/ (client1_transcript.txt, client2_transcript.txt, concurrent_*.txt, functional_test.txt, server.log). README done.

## Cluster jobs (all finished)
- 93264 hw3-hadoop: SSSP bench + weather MR matrix complete (all verified); hit 8 h TIMEOUT during the MPI phase because mpirun cannot launch inside the Hadoop job (daemon step holds all task slots).
- 93427 hw3-mpi: MPI re-run as its own job (section2/q1_mapreduce/mpi_bench.sbatch, PHASE=mpi) — complete, 60 runs, all match.
- 93277 grpc series D+E rerun — complete.

## (historical)
- 93264 hw3-hadoop: hadoop/bench_all.sh (SSSP bench then weather MR + MPI). Log ~/HW3/logs/hadoop_93264.log. Started 22:15, expect done ~02:00. SSSP part: sample/rnd1k/rnd10k(22 it)/rnd10k_dense(19 it) all match Dijkstra so far.
- 93273 hw3-grpc SERIES=D: reruns only the concurrent-query series with in-stream ("active") latency stats (old D rows removed from grpc_bench.csv; full old CSV kept as grpc_bench_run2_full.csv). Log ~/HW3/logs/grpc_93273.log.
- 93269 (gRPC series A–E) DONE: results pulled → section2/q2_grpc/results/cluster/grpc_bench.csv, figures + tables.md generated by plot.py. One failed config (W=12 small rep0: slurm step launch lag; fixed timeout to 240 s in bench.py).

## READMEs status
- section3/problem1/README.md: DONE (cluster transcripts in demo/).
- section2/q2_grpc/README.md: **COMPLETE**. Series D+E were rerun (job 93277) after fixing two measurement bugs: (1) streamer --preload now parses BEFORE opening the RPC (stream_active used to cover the parse phase → fake 1.4 ms query medians); (2) query_load reports 'active' (in-stream) stats with real time span. True numbers: idle query 12 ms (small) / 25 ms (medium); during saturated ingest ~53 ms, ~30 QPS max, p50 linear in clients; ingest loses 25–40 %. Nothing left to do here. → replace with results/cluster/tables.md content, figure links (results/cluster/fig/grpc_*.png) and analysis. Key findings: W=1 worker-bound ~500k rec/s; W≥2 coordinator-bound ~1.05M rec/s (single Python ingest thread; GIL); live text parsing caps ~750k; batch=1 → 10k rec/s (~0.1 ms/message), ≥1000 flat; station-hash costs coordinator CPU (2.1→7.3 s) and drops throughput (esp. W=8); back-to-back query clients compete for the coordinator GIL (ingest drops, p99 grows with clients); throttled 200k stream + 4 clients: p50 3.4 ms p99 51 ms.
- section1/q2_sssp/README.md: COMPLETE. (was: design DONE; `RESULTS_PLACEHOLDER` → tables.md + fig/sssp_*.png + analysis after job 93264. NOTE rnd1k iteration rows 1–12 were lost (sync mishap) — rerun `bash run_hadoop.sh data/rnd1k.txt ... --tag rnd1k` if a full per-iteration profile of rnd1k is wanted (rnd10k_dense profile is complete and is what plot.py uses).
- section2/q1_mapreduce/README.md: COMPLETE. (was: design DONE; `RESULTS_PLACEHOLDER` → tables.md + fig/wx_*.png + MPI-vs-MR analysis after job 93264.
- hadoop/README.md, top-level README.md: DONE.

## Plotting
- `tools/plotstyle.py` shared style. `section1/q2_sssp/plot.py`, `section2/q1_mapreduce/plot.py`, `section2/q2_grpc/plot.py` each read `results/cluster/*.csv` → `results/cluster/fig/*.png` + `results/cluster/tables.md`. Run with `~/hw3venv/bin/python plot.py`.
- gRPC README written except the results section (marker `RESULTS_PLACEHOLDER` at the end → replace with tables.md content + figure links + analysis).

## Remaining work
None. All four READMEs have design + correctness + results + analysis; figures in `*/results/cluster/fig/`; cluster copy synced.
Optional polish if time: rerun `rnd1k` SSSP graph to recover its lost per-iteration rows (only affects a table, plot uses rnd10k_dense).
