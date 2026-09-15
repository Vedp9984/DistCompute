#!/usr/bin/env python3
"""Concurrent query load generator.

    python3 bench/query_load.py <coordinator addr> --clients C [--duration S | --until-complete]
                                [--think 0] [--csv FILE --tag NAME]

Each client is a thread with its own channel issuing GetAnalytics back-to-back
(optionally with a think time).  Reports QPS and latency percentiles.
"""
import argparse
import csv
import os
import statistics
import sys
import threading
import time

import grpc

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import weather_pb2 as pb          # noqa: E402
import weather_pb2_grpc as pb_grpc  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("address")
    ap.add_argument("--clients", type=int, default=1)
    ap.add_argument("--duration", type=float, default=10.0)
    ap.add_argument("--until-complete", action="store_true", help="stop when the stream completes")
    ap.add_argument("--think", type=float, default=0.0)
    ap.add_argument("--csv", default="")
    ap.add_argument("--tag", default="")
    a = ap.parse_args()

    lat = [[] for _ in range(a.clients)]        # (latency, stream_active) per query
    stop = threading.Event()
    seen_active = threading.Event()

    def client(i):
        stub = pb_grpc.QueryServiceStub(grpc.insecure_channel(a.address))
        req = pb.QueryRequest()
        while not stop.is_set():
            t0 = time.perf_counter()
            r = stub.GetAnalytics(req)
            t1 = time.perf_counter()
            lat[i].append((t1 - t0, r.stream_active, t0, t1))
            if r.stream_active:
                seen_active.set()
            if a.until_complete and r.stream_complete and seen_active.is_set():
                stop.set()
            if a.think:
                time.sleep(a.think)

    t_start = time.perf_counter()
    ths = [threading.Thread(target=client, args=(i,), daemon=True) for i in range(a.clients)]
    for t in ths:
        t.start()
    deadline = t_start + a.duration
    while not stop.is_set() and (a.until_complete or time.perf_counter() < deadline):
        time.sleep(0.05)
        if a.until_complete and time.perf_counter() > t_start + 3600:
            break
    stop.set()
    for t in ths:
        t.join(timeout=5)
    wall = time.perf_counter() - t_start
    everything = [x for l in lat for x in l]
    if not everything:
        print("no queries completed")
        return

    def report(label, samples, span):
        s = sorted(x[0] for x in samples)
        n = len(s)
        if n == 0:
            print(f"{label} clients={a.clients} queries=0")
            return
        pct = lambda p: s[min(n - 1, int(p * n))] * 1e3
        print(f"{label} clients={a.clients} queries={n} wall={span:.1f}s qps={n/span:.1f} "
              f"latency ms: mean={statistics.mean(s)*1e3:.1f} p50={pct(0.5):.1f} p95={pct(0.95):.1f} p99={pct(0.99):.1f} max={s[-1]*1e3:.1f}")
        return s

    all_lat = report("all", everything, wall)
    # Only the queries answered while the stream was flowing: these are the
    # numbers that describe "querying during ingest".  Idle-time queries are
    # cheap (empty tables) and would drag the median down.
    active = [x for x in everything if x[1]]
    active_span = (max(x[3] for x in active) - min(x[2] for x in active)) if active else 0.0
    report("active", active, max(active_span, 1e-9))
    if a.csv:
        new = not os.path.exists(a.csv)
        with open(a.csv, "a", newline="") as fh:
            w = csv.writer(fh)
            if new:
                w.writerow(["tag", "clients", "queries", "wall_s", "qps", "mean_ms", "p50_ms", "p95_ms", "p99_ms", "max_ms"])
            w.writerow([a.tag, a.clients, n, f"{wall:.3f}", f"{n/wall:.2f}", f"{statistics.mean(all_lat)*1e3:.2f}",
                        f"{pct(0.5):.2f}", f"{pct(0.95):.2f}", f"{pct(0.99):.2f}", f"{all_lat[-1]*1e3:.2f}"])


if __name__ == "__main__":
    main()
