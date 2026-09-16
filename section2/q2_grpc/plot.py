#!/usr/bin/env python3
"""Figures + markdown tables for the gRPC streaming benchmark.
    python3 plot.py [results_dir]      (default results/cluster)
"""
import csv
import os
import statistics as st
import sys
from collections import defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "tools"))
import plotstyle as ps  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402

R = sys.argv[1] if len(sys.argv) > 1 else "results/cluster"
FIG = os.path.join(R, "fig"); os.makedirs(FIG, exist_ok=True)
with open(os.path.join(R, "grpc_bench.csv")) as fh:
    rows = list(csv.DictReader(fh))
for r in rows:
    for k in ("workers", "batch", "query_clients", "records", "preload"):
        r[k] = int(r[k])
    for k in ("coord_rps", "coord_elapsed_s", "stream_wall_s", "coord_cpu_s", "worker_cpu_s_sum", "worker_rss_mb_max", "coord_rss_mb", "rate"):
        r[k] = float(r[k] or 0)
    for k in ("qps", "q_mean_ms", "q_p50_ms", "q_p95_ms", "q_p99_ms"):
        r[k] = float(r[k]) if r[k] else None

def sel(tag, **kw):
    out = [r for r in rows if r["tag"] == tag and all(r[k] == v for k, v in kw.items())]
    return out

def med(rs, k):
    v = [r[k] for r in rs if r[k] is not None]
    return st.median(v) if v else float("nan")

md = ["# gRPC streaming — results\n", f"All runs: {sum(1 for r in rows if r['final_match']=='yes')}/{len(rows)} final reports byte-identical to `weather_seq`.\n"]

# ---- A: workers -----------------------------------------------------------------
md += ["## A. Number of workers (batch 1000, round-robin, pre-parsed stream)\n", "| dataset | workers | throughput rec/s | processing time s | coordinator CPU s | Σ worker CPU s | worker RSS MB |", "|---|---|---|---|---|---|---|"]
fig, ax = plt.subplots(figsize=(7, 3.8))
for i, ds in enumerate(["weather_small.txt", "weather_medium.txt"]):
    ws = sorted({r["workers"] for r in sel("A_workers", dataset=ds)})
    if not ws:
        continue
    ys = [med(sel("A_workers", dataset=ds, workers=w), "coord_rps") for w in ws]
    ax.plot(ws, ys, marker="o", color=ps.color(i), label=f"{ds.replace('weather_','').replace('.txt','')} ({sel('A_workers', dataset=ds)[0]['records']:,} records), pre-parsed")
    for w in ws:
        rs = sel("A_workers", dataset=ds, workers=w)
        md.append(f"| {ds} | {w} | {med(rs,'coord_rps'):,.0f} | {med(rs,'coord_elapsed_s'):.2f} | {med(rs,'coord_cpu_s'):.1f} | {med(rs,'worker_cpu_s_sum'):.1f} | {med(rs,'worker_rss_mb_max'):.0f} |")
live = sel("A2_live")
if live:
    ws = sorted({r["workers"] for r in live})
    ax.plot(ws, [med(sel("A2_live", workers=w), "coord_rps") for w in ws], marker="s", linestyle="--", color=ps.color(2), label="medium, streamer parsing text live")
    md += ["\n### A2. Same, but the streamer parses the text file while sending\n", "| workers | throughput rec/s |", "|---|---|"]
    md += [f"| {w} | {med(sel('A2_live', workers=w),'coord_rps'):,.0f} |" for w in ws]
ax.set_xscale("log", base=2); ax.set_xticks([1, 2, 4, 8, 12]); ax.set_xticklabels(["1", "2", "4", "8", "12"])
ax.set_ylim(bottom=0); ax.set_xlabel("workers"); ax.set_ylabel("records / s (coordinator-side)"); ax.set_title("Ingest throughput vs number of workers"); ax.legend(fontsize=8)
ps.finish(fig, os.path.join(FIG, "grpc_workers.png"), "One worker is the bottleneck at W=1; from W=2 the single-threaded coordinator (~1.05 M rec/s) is, and a live text parser caps lower still.")

# ---- B: batch --------------------------------------------------------------------
bs = sorted({r["batch"] for r in sel("B_batch")})
if bs:
    md += ["\n## B. Message granularity (4 workers, round-robin, small dataset)\n", "| records / message | messages | throughput rec/s | processing time s | coordinator CPU s |", "|---|---|---|---|---|"]
    for b in bs:
        rs = sel("B_batch", batch=b)
        md.append(f"| {b} | {rs[0]['records']//b:,} | {med(rs,'coord_rps'):,.0f} | {med(rs,'coord_elapsed_s'):.2f} | {med(rs,'coord_cpu_s'):.1f} |")
    fig, ax = plt.subplots(figsize=(7, 3.8))
    ax.plot(bs, [med(sel("B_batch", batch=b), "coord_rps") for b in bs], marker="o", color=ps.color(0))
    for b in bs:
        y = med(sel("B_batch", batch=b), "coord_rps"); ax.annotate(f"{y:,.0f}", (b, y), textcoords="offset points", xytext=(0, 8), ha="center", fontsize=8, color=ps.TEXT2)
    ax.set_xscale("log"); ax.set_yscale("log"); ax.set_xlabel("records per gRPC message"); ax.set_ylabel("records / s"); ax.set_title("Throughput vs message granularity (W=4)")
    ps.finish(fig, os.path.join(FIG, "grpc_batch.png"), "Per-message cost ≈ 0.1 ms end to end: 1 record/message → 10 k rec/s; the curve flattens once a message carries ≥ 1 000 records.")

# ---- C: distribution -----------------------------------------------------------------
cw = sorted({r["workers"] for r in sel("C_dist")})
if cw:
    md += ["\n## C. Distribution strategy (batch 1000, medium dataset)\n", "| workers | round-robin rec/s | station-hash rec/s | coordinator CPU s (rr / station) |", "|---|---|---|---|"]
    fig, ax = plt.subplots(figsize=(6.5, 4.2))
    x = range(len(cw)); wd = 0.34
    for i, d in enumerate(["rr", "station"]):
        ax.bar([xx + (i - 0.5) * (wd + 0.03) for xx in x], [med(sel("C_dist", workers=w, dist=d), "coord_rps") for w in cw], width=wd, color=ps.color(i), label={"rr": "round-robin batches", "station": "hash by station id"}[d])
    for w in cw:
        rr, stn = sel("C_dist", workers=w, dist="rr"), sel("C_dist", workers=w, dist="station")
        md.append(f"| {w} | {med(rr,'coord_rps'):,.0f} | {med(stn,'coord_rps'):,.0f} | {med(rr,'coord_cpu_s'):.1f} / {med(stn,'coord_cpu_s'):.1f} |")
    ax.set_xticks(list(x)); ax.set_xticklabels([f"W={w}" for w in cw]); ax.set_ylabel("records / s"); ax.set_title("Record distribution strategy (batch 1000, 5 M records)")
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.12), ncol=2)
    ps.finish(fig, os.path.join(FIG, "grpc_dist.png"), "Hashing by station makes the coordinator re-partition every batch in Python; the bottleneck stage gets more work, so throughput drops.")

# ---- D: queries --------------------------------------------------------------------------
qc = sorted({r["query_clients"] for r in sel("D_queries")})
if qc:
    md += ["\n## D. Concurrent query clients during ingest (4 workers, batch 1000, medium)\n", "| dist | query clients | ingest rec/s | queries served | QPS | latency p50 ms | p95 ms | p99 ms |", "|---|---|---|---|---|---|---|---|"]
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(10, 3.8))
    for i, d in enumerate(["rr", "station"]):
        base = med(sel("C_dist", workers=4, dist=d), "coord_rps")
        xs = [0] + qc
        a1.plot(xs, [base] + [med(sel("D_queries", query_clients=q, dist=d), "coord_rps") for q in qc], marker="o", color=ps.color(i), label=f"ingest throughput, {d}")
        a2.plot(qc, [med(sel("D_queries", query_clients=q, dist=d), "q_p50_ms") for q in qc], marker="o", color=ps.color(i), label=f"p50, {d}")
        a2.plot(qc, [med(sel("D_queries", query_clients=q, dist=d), "q_p99_ms") for q in qc], marker="^", linestyle="--", color=ps.color(i), label=f"p99, {d}")
        for q in qc:
            rs = sel("D_queries", query_clients=q, dist=d)
            md.append(f"| {d} | {q} | {med(rs,'coord_rps'):,.0f} | {med(rs,'queries'):.0f} | {med(rs,'qps'):.0f} | {med(rs,'q_p50_ms'):.1f} | {med(rs,'q_p95_ms'):.1f} | {med(rs,'q_p99_ms'):.1f} |") if False else md.append(f"| {d} | {q} | {med(rs,'coord_rps'):,.0f} | {rs[0]['queries'] if rs else ''} | {med(rs,'qps'):.0f} | {med(rs,'q_p50_ms'):.1f} | {med(rs,'q_p95_ms'):.1f} | {med(rs,'q_p99_ms'):.1f} |")
    a1.set_ylim(bottom=0); a1.set_xlabel("concurrent query clients (0 = none)"); a1.set_ylabel("records / s"); a1.set_title("Ingest throughput under query load"); a1.legend(fontsize=8)
    a2.set_xlabel("concurrent query clients"); a2.set_ylabel("query latency (ms)"); a2.set_title("Query latency under load"); a2.legend(fontsize=8, ncol=2)
    ps.finish(fig, os.path.join(FIG, "grpc_queries.png"), "Every query fans out GetPartial to all workers and merges S + R table entries; it competes with ingest for the coordinator's CPU.")

# ---- E: throttled ------------------------------------------------------------------------------
e = sel("E_rate")
if e:
    md += ["\n## E. Throttled source (200 000 rec/s target, 4 query clients)\n", f"achieved {med(e,'coord_rps'):,.0f} rec/s; queries: QPS {med(e,'qps'):.0f}, p50 {med(e,'q_p50_ms'):.1f} ms, p99 {med(e,'q_p99_ms'):.1f} ms\n"]

with open(os.path.join(R, "tables.md"), "w") as fh:
    fh.write("\n".join(md) + "\n")
print("wrote tables.md")
