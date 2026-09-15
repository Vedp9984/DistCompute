#!/usr/bin/env python3
"""Benchmark harness for the gRPC streaming system.

    python3 bench/bench.py --dataset DATA --out results/grpc_bench.csv
        [--workers 1,2,4,8] [--batch 1,10,100,1000,10000] [--dist rr,station]
        [--queries 0,1,4,8] [--rate 0] [--repeat 1]
        [--mode local | --mode slurm --coord-node node01 --worker-nodes node02,node03 --client-node node04]

For every configuration it starts the workers and the coordinator, launches the
optional concurrent query clients, replays the dataset with the streamer,
waits for completion, verifies the final report against EXPECTED (if given),
records everything as one CSV row, and tears the processes down.

slurm mode launches every process with `srun --overlap -N1 -n1 -w NODE` inside
the current allocation, so workers really run on other machines.
"""
import argparse
import csv
import itertools
import os
import shlex
import signal
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
PY = os.environ.get("PY", sys.executable)


class Launcher:
    def __init__(self, mode, coord_node, worker_nodes, client_node):
        self.mode = mode
        self.coord_node = coord_node
        self.worker_nodes = worker_nodes
        self.client_node = client_node
        self.procs = []

    def _wrap(self, node, cmd, cpus=2):
        if self.mode == "local" or not node:
            return cmd
        return ["srun", "--overlap", "-N1", "-n1", "-w", node, f"--cpus-per-task={cpus}",
                "--export=ALL", "--quiet"] + cmd

    def host(self, node):
        return "localhost" if self.mode == "local" else node

    def spawn(self, node, cmd, log, cpus=2):
        full = self._wrap(node, cmd, cpus)
        fh = open(log, "ab")
        p = subprocess.Popen(full, stdout=fh, stderr=subprocess.STDOUT, cwd=ROOT,
                             preexec_fn=os.setsid)
        self.procs.append((p, fh))
        return p

    def run(self, node, cmd, cpus=2, timeout=3600):
        return subprocess.run(self._wrap(node, cmd, cpus), cwd=ROOT, capture_output=True, text=True, timeout=timeout)

    def killall(self):
        for p, fh in self.procs:
            try:
                os.killpg(os.getpgid(p.pid), signal.SIGTERM)
            except ProcessLookupError:
                pass
        for p, fh in self.procs:
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(os.getpgid(p.pid), signal.SIGKILL)
            fh.close()
        self.procs.clear()


def wait_port(host, port, timeout=240):   # slurm step launch can lag by minutes under load
    import socket
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except OSError:
            time.sleep(0.3)
    return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--expected", default="", help="weather_seq report to diff the final report against")
    ap.add_argument("--out", default="results/grpc_bench.csv")
    ap.add_argument("--workers", default="1,2,4,8")
    ap.add_argument("--batch", default="1000")
    ap.add_argument("--dist", default="rr")
    ap.add_argument("--queries", default="0")
    ap.add_argument("--rate", type=float, default=0)
    ap.add_argument("--repeat", type=int, default=1)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--mode", choices=["local", "slurm"], default="local")
    ap.add_argument("--coord-node", default="")
    ap.add_argument("--worker-nodes", default="")
    ap.add_argument("--client-node", default="")
    ap.add_argument("--base-port", type=int, default=60060)
    ap.add_argument("--tag", default="")
    ap.add_argument("--preload", action="store_true", help="streamer pre-parses the file (measures the pipeline, not the parser)")
    a = ap.parse_args()

    worker_nodes = a.worker_nodes.split(",") if a.worker_nodes else [""]
    L = Launcher(a.mode, a.coord_node, worker_nodes, a.client_node)
    os.makedirs(os.path.join(ROOT, "logs"), exist_ok=True)
    os.makedirs(os.path.dirname(os.path.join(ROOT, a.out)) or ".", exist_ok=True)
    out_path = os.path.join(ROOT, a.out)
    new = not os.path.exists(out_path)
    fh = open(out_path, "a", newline="")
    w = csv.writer(fh)
    if new:
        w.writerow(["tag", "dataset", "records", "workers", "batch", "dist", "query_clients", "rate", "preload", "rep",
                    "stream_wall_s", "streamer_rps", "coord_rps", "coord_elapsed_s", "e2e_s",
                    "queries", "qps", "q_mean_ms", "q_p50_ms", "q_p95_ms", "q_p99_ms",
                    "coord_cpu_s", "coord_rss_mb", "worker_cpu_s_sum", "worker_rss_mb_max", "final_match"])
    configs = list(itertools.product(
        [int(x) for x in a.workers.split(",")],
        [int(x) for x in a.batch.split(",")],
        a.dist.split(","),
        [int(x) for x in a.queries.split(",")],
        range(a.repeat)))
    coord_host = L.host(a.coord_node)
    coord_port = a.base_port - 9   # 60051 by default

    for W, B, D, Q, rep in configs:
        label = f"W={W} batch={B} dist={D} queries={Q} rep={rep}"
        print(f"=== {label}", flush=True)
        try:
            addrs = []
            for i in range(W):
                node = worker_nodes[i % len(worker_nodes)]
                port = a.base_port + i
                L.spawn(node, [PY, "worker.py", "--port", str(port), "--id", f"w{i}"], f"logs/bench_worker_{i}.log", cpus=1)
                addrs.append(f"{L.host(node)}:{port}")
            for adr in addrs:
                h, p = adr.split(":")
                if not wait_port(h, int(p)):
                    raise RuntimeError(f"worker {adr} did not come up")
            L.spawn(a.coord_node, [PY, "coordinator.py", "--port", str(coord_port), "--workers", ",".join(addrs),
                                   "--dist", D], "logs/bench_coordinator.log", cpus=4)
            if not wait_port(coord_host, coord_port):
                raise RuntimeError("coordinator did not come up")
            time.sleep(0.5)
            caddr = f"{coord_host}:{coord_port}"

            qproc = None
            qlog = os.path.join(ROOT, "logs", "bench_query.log")
            if Q > 0:
                if os.path.exists(qlog):
                    os.remove(qlog)
                qproc = L.spawn(a.client_node, [PY, "bench/query_load.py", caddr, "--clients", str(Q),
                                                "--until-complete"], qlog, cpus=max(2, Q))
                time.sleep(0.5)

            t0 = time.perf_counter()
            cmd = [PY, "streamer.py", caddr, a.dataset, "--batch", str(B), "--rate", str(a.rate)]
            if a.limit:
                cmd += ["--limit", str(a.limit)]
            if a.preload:
                cmd.append("--preload")
            r = L.run(a.client_node, cmd, cpus=2)
            line = [l for l in r.stdout.splitlines() if l.startswith("streamed")]
            if not line:
                print(r.stdout, r.stderr)
                raise RuntimeError("streamer failed")
            line = line[0]
            print("   " + line, flush=True)
            toks = line.split()
            records = int(toks[1])
            wall = float(toks[toks.index("wall") + 1].rstrip("s"))
            s_rps = float(toks[toks.index("->") + 1].replace(",", ""))
            c_rps = float(toks[toks.index("(streamer)") + 1].replace(",", ""))
            c_el = float(toks[-1].rstrip("s)"))

            fin = None
            for attempt in range(3):
                fin = L.run(a.client_node, [PY, "dashboard.py", caddr, "--final", "--wait"], cpus=1)
                if fin.stdout.strip():
                    break
                print(f"   (final report attempt {attempt+1} returned nothing: {fin.stderr.strip()[-200:]})", flush=True)
                time.sleep(1)
            e2e = time.perf_counter() - t0
            match = ""
            if a.expected:
                exp = open(a.expected).read()
                match = "yes" if fin.stdout == exp else "NO"
                if match == "NO":
                    with open(os.path.join(ROOT, "logs", f"bench_mismatch_W{W}_B{B}_{D}_Q{Q}.txt"), "w") as mf:
                        mf.write(fin.stdout)
            print(f"   e2e {e2e:.2f}s  final_match={match or 'n/a'}", flush=True)

            qstats = ["", "", "", "", "", ""]
            if qproc is not None:
                try:
                    qproc.wait(timeout=120)
                except subprocess.TimeoutExpired:
                    os.killpg(os.getpgid(qproc.pid), signal.SIGTERM)
                lines = open(qlog).read().splitlines()
                # prefer the in-stream ("active") statistics; fall back to "all"
                pick = [l for l in lines if l.startswith("active ") and "queries=0" not in l] or [l for l in lines if l.startswith("all ")]
                for l in lines:
                    if l.startswith(("all ", "active ")):
                        print("   " + l, flush=True)
                if pick:
                    kv = dict(t.split("=") for t in pick[0].replace("latency ms: ", "").split() if "=" in t)
                    qstats = [kv["queries"], kv["qps"], kv["mean"], kv["p50"], kv["p95"], kv["p99"]]

            raw = L.run(a.client_node, [PY, "dashboard.py", caddr, "--raw"], cpus=1).stdout
            def grab(key):
                for l in raw.splitlines():
                    if l.strip().startswith(key + ":"):
                        return float(l.split(":")[1])
                return 0.0
            coord_cpu = grab("coord_cpu_seconds"); coord_rss = grab("coord_rss_bytes") / 2**20
            wcpu, wrss = 0.0, 0.0
            block = False
            for l in raw.splitlines():
                s = l.strip()
                if s.startswith("workers {"):
                    block = True
                elif block and s.startswith("cpu_seconds:"):
                    wcpu += float(s.split(":")[1])
                elif block and s.startswith("rss_bytes:"):
                    wrss = max(wrss, float(s.split(":")[1]) / 2**20)
            w.writerow([a.tag, os.path.basename(a.dataset), records, W, B, D, Q, a.rate, int(a.preload), rep,
                        f"{wall:.3f}", f"{s_rps:.0f}", f"{c_rps:.0f}", f"{c_el:.3f}", f"{e2e:.3f}", *qstats,
                        f"{coord_cpu:.2f}", f"{coord_rss:.0f}", f"{wcpu:.2f}", f"{wrss:.0f}", match])
            fh.flush()
        except Exception as e:
            print(f"   FAILED: {e}", flush=True)
        finally:
            L.killall()
            time.sleep(1.0)
    fh.close()


if __name__ == "__main__":
    main()
