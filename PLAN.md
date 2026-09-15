# HW3 — Implementation Plan

Assigned scope:
- **Section 1 → Q2**: Single Source Shortest Path (SSSP), iterative MapReduce, C++ Hadoop Streaming
- **Section 2 → Q8 (weather analytics, from HW2)**: Q1 = Hadoop MapReduce (C++ streaming, mandated), Q2 = gRPC real-time streaming (Python)
- **Section 3 → Problem 1**: Collaborative Document Editing over gRPC (Python: server.py / client.py)

Cluster: ADA (Slurm). Hadoop 3.3.6 + YARN must be self-hosted on allocated compute nodes unless a module exists.

---

## 0. Directory layout

```
Distri_hw_3/
├── PLAN.md
├── hadoop/                     # cluster-side Hadoop 3.3.6 bring-up (shared by Sec1 + Sec2-Q1)
│   ├── install_hadoop.sh       # fetch tarball → ~/hadoop-3.3.6, set env
│   ├── gen_config.sh           # writes core/hdfs/yarn/mapred-site.xml + workers from $SLURM_JOB_NODELIST
│   ├── start_cluster.sh        # format NN (first time), start dfs + yarn, wait for NMs
│   ├── stop_cluster.sh
│   └── hadoop_job.sbatch       # salloc/sbatch wrapper: bring up cluster → run a given script → tear down
├── section1/q2_sssp/
│   ├── src/  prep.cpp  mapper.cpp  combiner.cpp  reducer.cpp  final.cpp
│   ├── run_local.sh            # pipe-based emulation of the iterative job (mapper|sort|reducer loop)
│   ├── run_hadoop.sh           # driver: prep job → loop streaming jobs until counter UPDATED==0 → final job
│   ├── gen_graph.py  oracle.py (Dijkstra)  verify.sh  bench.sh
│   ├── tests/  results/  README.md
├── section2/q1_mapreduce/
│   ├── src/  weather_common.hpp (from HW2)  wx_mapper.cpp  wx_combiner.cpp  wx_reducer.cpp  wx_final.cpp
│   ├── run_local.sh  run_hadoop.sh  verify.sh  bench.sh  plot.py
│   ├── results/  README.md     # + MPI vs MR comparison
├── section2/q2_grpc/
│   ├── proto/weather.proto
│   ├── coordinator.py  worker.py  streamer.py  dashboard.py  common.py (accumulator + merge + report)
│   ├── launch_cluster.sh (Slurm: coordinator on node1, workers spread), bench/, results/, README.md
├── section3/problem1/
│   ├── document.proto  server.py  client.py  concurrent_demo.py  README.md
└── q8_from hw2/                # unchanged: gen_weather + weather_seq reused as generator + oracle
```

---

## 1. Section 1 Q2 — SSSP iterative MapReduce (C++ streaming)

**Record format** (one line per vertex, after prep):
`node \t dist \t v1:w1,v2:w2,...`   with `dist = INF` sentinel (`-1`) for unreached.

- `prep.cpp` (single MR job, or local): edge list → adjacency records for **all** vertices 0..V-1
  (vertices with no out-edges / only in-edges still get a record so they appear in output).
  Header line "V E" is skipped by field count. Node 0 gets dist 0.
- `mapper.cpp`: for each record emit
  `node \t N|dist|adj` (carry the graph structure forward) and, if dist != INF,
  `v \t D|dist+w` for every out-edge.
- `combiner.cpp`: per key keep min of D values, pass N record through.
- `reducer.cpp`: per key: new_dist = min(old dist, min D). Emit updated record.
  If new_dist < old dist → `reporter:counter:SSSP,UPDATED,1` on stderr.
- `run_hadoop.sh`: loop `hadoop jar hadoop-streaming.jar ... -input iter_i -output iter_{i+1}`,
  read the `UPDATED` counter from job output, stop when 0 (bound: V-1 iterations).
- `final.cpp`: record → `node dist` (or `INF`), 1 reducer with
  `KeyFieldBasedComparator -n` so output is numerically sorted by node id.
- Correctness: `oracle.py` (Dijkstra) vs MR output on: PDF sample, V=2, disconnected nodes,
  cycles, multiple paths with different hop counts, random graphs up to V=10000/E=50000.
- Benchmark: iterations vs graph diameter, wall time per iteration, effect of #reducers.

## 2. Section 2 Q1 — Weather analytics, Hadoop MapReduce (C++ streaming)

Reuse `weather_common.hpp` (Accum, parse_line, KSum, tie-break comparators, write_report).

**Two-stage design** (default; single-stage with 1 reducer also supported for comparison):

- **Job 1 mapper** (`wx_mapper`): in-mapper combining. Parse every line into a local `Accum`
  (header line "N K S" skipped by field count). At EOF emit:
  - `G \t count extreme sum_t sum_h sum_p sum_rain sum_wind min/max… hottest coldest`  (1 line)
  - `S<id> \t count temp_sum rain_sum`   (one per station seen)
  - `I<bucket> \t count`                  (one per 60-s interval seen)
  Doubles emitted as `%a` hex floats → exact round-trip, so KSum precision is preserved.
- **Job 1 combiner/reducer** (`wx_reducer`, R configurable): merge lines by key
  (sum / min / max / tie-break comparators). Output = same format, one line per key.
- **Job 2** (`wx_final`, identity mapper, 1 reducer): consumes the merged partials, builds
  `Report`, applies top-K (K passed via `-cmdenv WX_K=…`, read from header by the driver),
  picks busiest interval, prints the exact HW2 report block.
- Correctness: `diff` vs `weather_seq` on `tests/*.txt` and generated small/medium; plus `oracle.py`.
- Benchmarks (cluster): small/medium/large(/xlarge) × mappers {via split size: 4,8,16,32}
  × reducers {1,4,8}; stage-wise times from job counters; HDFS put time separately.
- **MPI vs MR**: rerun HW2 `weather_mpi` P∈{1,2,4,8} on the same nodes; tables + plots:
  time vs N, time vs tasks, throughput (MB/s), speedup; qualitative: I/O path (HDFS vs
  byte-range POSIX), startup overhead (JVM/YARN ~10–20 s vs ms), fault tolerance, code size.

## 3. Section 2 Q2 — Weather analytics as a gRPC streaming system (Python)

**Architecture**
```
streamer.py ──stream RecordBatch──▶ coordinator.py ──dispatch──▶ worker.py ×W
                                         ▲     (round-robin | hash(station))
dashboard.py ──GetAnalytics──────────────┘  fan-out GetPartial → merge → report
```
- `weather.proto`: `Record`, `RecordBatch{repeated Record}`, `IngestService.StreamRecords(stream RecordBatch) → IngestSummary`,
  `WorkerService.Process(stream RecordBatch) → Ack`, `WorkerService.GetPartial(Empty) → PartialState`,
  `QueryService.GetAnalytics(QueryRequest) → AnalyticsReport` (+ `GetSystemStats`: throughput, per-worker counts).
- `worker.py`: holds partial state (counts, compensated sums via `math.fsum`-style
  accumulation, min/max, hottest/coldest with tie-breaks, per-station dict, interval dict);
  one lock around update/snapshot.
- `coordinator.py`: receives stream, forwards batches to workers over persistent streams;
  distribution strategy flag `--dist rr|station`; answers queries by pulling partials and
  merging (same merge code as `common.py`), so queries work mid-ingest.
- `streamer.py`: `--batch N`, `--rate records/s` (0 = unthrottled), `--seed`, reads HW2 dataset.
- `dashboard.py`: curses-free CLI refresh loop (`--interval`), shows report + ingestion rate +
  per-worker load; `--once --final` prints the exact HW2 report for `diff` vs `weather_seq`.
- Experiments: workers {1,2,4,8} × batch {1,10,100,1000,10000} × concurrent query clients
  {0,1,4,8} × distribution {rr,station}. Metrics: records/s, end-to-end time, query latency
  p50/p99, CPU/RSS via psutil. Cluster: coordinator node1, workers on node2/3, streamer+dashboard node4.

## 4. Section 3 Problem 1 — Collaborative Document Editing (gRPC, Python)

- `document.proto`: `DocumentService { CreateDocument, GetDocument, EditDocument, SubscribeToUpdates(stream) }`.
- `server.py <addr>`: in-memory `{name: Document(content, lock, subscribers)}`; global lock for
  create; per-document lock for edit; each subscriber has a `queue.Queue`, server-streaming RPC
  drains it; dead subscribers removed on `context.is_active()==False`. Errors via
  `NOT_FOUND`, `ALREADY_EXISTS`, `OUT_OF_RANGE` (bad position), `INVALID_ARGUMENT`.
- `client.py <addr>`: menu 1–5 + shorthand commands (`create/open/edit/subscribe/exit`),
  subscription runs in a daemon thread printing `[Update] …` while the prompt stays live.
- `concurrent_demo.py`: two clients fire `edit report.txt 6 …` simultaneously → show both
  applied, document intact.
- README: run instructions + transcript of the 6-step demo captured on 3 ADA nodes.

## 5. Cluster (ADA) bring-up plan

1. Login, probe: `module avail 2>&1 | grep -i -E 'hadoop|java|python'`, `java -version`,
   `python3 -m pip --version`, quota, `/scratch` availability, whether compute nodes can ssh each other.
2. Hadoop: if no module → `install_hadoop.sh` (tarball to `~/hadoop-3.3.6`; Java 8/11 required).
   `gen_config.sh` builds configs from the salloc nodelist: NN + RM on node 1, DN + NM on all;
   HDFS/YARN local dirs on node-local `/scratch` or `/tmp` (never NFS home). Start with
   pseudo-distributed on 1 node to validate, then 4-node.
3. Python: `python3 -m venv ~/hw3venv && pip install grpcio grpcio-tools psutil`.
4. C++: `g++ -O2 -std=c++17 -static-libstdc++` so streaming binaries run on every NodeManager.
5. Sync: `rsync` the repo to `~/HW3/`; datasets are regenerated on the cluster (`gen_weather`, `gen_graph.py`).

## 6. Order of work

1. Local dev environment: Hadoop 3.3.6 pseudo-distributed on this laptop (Java 11 present), `pip install grpcio grpcio-tools`.
2. Section 3 P1 (small, self-contained) — local test with 3 terminals.
3. Section 1 Q2 — build + `run_local.sh` verify → Hadoop local → cluster.
4. Section 2 Q1 — build + verify vs `weather_seq` locally → cluster benchmarks.
5. Section 2 Q2 — local multi-process test → cluster benchmarks.
6. Cluster: Hadoop bring-up, all benchmarks, MPI rerun, demo transcripts.
7. READMEs, plots (matplotlib), final packaging.
