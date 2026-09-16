#!/usr/bin/env python3
"""Figures + markdown tables for the weather MapReduce benchmark and the MPI comparison.
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
SIZES = ["small", "medium", "large", "xlarge"]
NREC = {"small": 1e6, "medium": 5e6, "large": 20e6, "xlarge": 50e6}

def load(name):
    p = os.path.join(R, name)
    if not os.path.exists(p):
        return []
    with open(p) as fh:
        return list(csv.DictReader(fh))

mr = [r for r in load("hadoop_runs.csv") if r["tag"] in SIZES]
seq = load("seq_runs.csv")
mpi = load("mpi_runs.csv")
put = load("hdfs_put.csv")
sizes = [s for s in SIZES if any(r["tag"] == s for r in mr)]
bytes_of = {r["tag"]: int(r["bytes"]) for r in mr}
md = ["# Weather analytics — MapReduce vs MPI results\n"]

def med(vals):
    return st.median(vals) if vals else float("nan")

# ---- per-size tables -------------------------------------------------------
seq_t = {s: min([float(r["seq_s"]) for r in seq if r["size"] == s]) for s in sizes}   # best of n: first run pays the NFS cold cache
put_t = {r["size"]: float(r["put_s"]) for r in put}
md += ["## Inputs\n", "| size | records | bytes | sequential T1 (s, best of 2) | HDFS put (s) |", "|---|---|---|---|---|"]
for s in sizes:
    md.append(f"| {s} | {int(NREC[s]):,} | {bytes_of.get(s, 0):,} | {seq_t.get(s, float('nan')):.2f} | {put_t.get(s, float('nan')):.1f} |")

def mr_rows(s, stages="2", comb="1"):
    return [r for r in mr if r["tag"] == s and r["stages"] == stages and r["combiner"] == comb]

md += ["\n## MapReduce, two-stage design: job time (s), median over repetitions\n"]
for s in sizes:
    rows = mr_rows(s)
    maps = sorted({int(r["maps_req"]) for r in rows}); reds = sorted({int(r["reducers"]) for r in rows})
    if not rows:
        continue
    md.append(f"**{s}** ({int(NREC[s]):,} records, sequential {seq_t.get(s, 0):.2f} s)\n")
    md.append("| maps \\ reducers | " + " | ".join(f"r={k}" for k in reds) + " | stage-1 s | final s | Σ map-task time s |")
    md.append("|---|" + "---|" * (len(reds) + 3))
    for m in maps:
        cells = []
        for k in reds:
            v = [float(r["total_s"]) for r in rows if int(r["maps_req"]) == m and int(r["reducers"]) == k]
            cells.append(f"{med(v):.1f}" if v else "–")
        ex = [r for r in rows if int(r["maps_req"]) == m and int(r["reducers"]) == reds[0]]
        s1 = med([float(r["stage1_s"]) for r in ex]); fn = med([float(r["final_s"]) for r in ex])
        cpu = med([float(r["map_task_ms"] or 0) / 1e3 for r in ex])
        md.append(f"| m={m} | " + " | ".join(cells) + f" | {s1:.1f} | {fn:.1f} | {cpu:.1f} |")
    md.append("")

# variants
md += ["## Design variants (8 maps)\n", "| size | two-stage r=4 (s) | one-stage: mapper → 1 reducer (s) | two-stage without combiner, r=4 (s) | map output records | shuffle bytes |", "|---|---|---|---|---|---|"]
for s in sizes:
    two = [r for r in mr_rows(s) if r["maps_req"] == "8" and r["reducers"] == "4"]
    one = [r for r in mr if r["tag"] == s and r["stages"] == "1"]
    noc = [r for r in mr if r["tag"] == s and r["combiner"] == "0"]
    f = lambda rows, k: med([float(r[k]) for r in rows if r[k]]) if rows else float("nan")
    md.append(f"| {s} | {f(two,'total_s'):.1f} | {f(one,'total_s'):.1f} | {f(noc,'total_s'):.1f} | {f(two,'map_output_records'):,.0f} | {f(two,'shuffle_bytes'):,.0f} |")

# ---- MPI table ---------------------------------------------------------------
Ps = sorted({int(r["P"]) for r in mpi})
if mpi:
    md += ["\n## MPI (HW2 program, same nodes): wall time (s), median of repetitions\n",
           "| size | " + " | ".join(f"P={p}" for p in Ps) + " |", "|---|" + "---|" * len(Ps)]
    for s in sizes:
        md.append(f"| {s} | " + " | ".join(f"{med([float(r['wall_s']) for r in mpi if r['size']==s and int(r['P'])==p]):.2f}" for p in Ps) + " |")

# ---- head-to-head ----------------------------------------------------------------
md += ["\n## Head-to-head: best configuration of each\n", "| size | sequential | MPI best (P) | MR best (maps, reducers) | MR / MPI | MR throughput MB/s | MPI throughput MB/s |", "|---|---|---|---|---|---|---|"]
best = {}
for s in sizes:
    m_best = min(((med([float(r["wall_s"]) for r in mpi if r["size"] == s and int(r["P"]) == p]), p) for p in Ps), default=(float("nan"), 0))
    rows = mr_rows(s)
    keys = sorted({(int(r["maps_req"]), int(r["reducers"])) for r in rows})
    r_best = min(((med([float(r["total_s"]) for r in rows if (int(r["maps_req"]), int(r["reducers"])) == k]), k) for k in keys), default=(float("nan"), (0, 0)))
    best[s] = (m_best, r_best)
    mb = bytes_of.get(s, 0) / 2**20
    md.append(f"| {s} | {seq_t.get(s, float('nan')):.2f} | {m_best[0]:.2f} (P={m_best[1]}) | {r_best[0]:.1f} (m={r_best[1][0]}, r={r_best[1][1]}) | {r_best[0]/m_best[0] if m_best[0] else float('nan'):.0f}× | {mb/r_best[0] if r_best[0] else 0:.0f} | {mb/m_best[0] if m_best[0] else 0:.0f} |")

with open(os.path.join(R, "tables.md"), "w") as fh:
    fh.write("\n".join(md) + "\n")
print("wrote tables.md")

# ---- Fig 1: time vs input size, MR vs MPI vs sequential (log-log) -------------------
if sizes:
    fig, ax = plt.subplots(figsize=(9, 4.2))
    xs = [NREC[s] for s in sizes]
    ax.plot(xs, [seq_t[s] for s in sizes], marker="o", color=ps.color(2), label="sequential (1 core)")
    for i, p in enumerate([p for p in Ps if p in (1, 8, 16)]):
        ys = [med([float(r["wall_s"]) for r in mpi if r["size"] == s and int(r["P"]) == p]) for s in sizes]
        ax.plot(xs, ys, marker="s", color=ps.color(0), alpha=1 - 0.3 * i, label=f"MPI P={p}")
    for i, (m, k) in enumerate([(2, 1), (8, 4), (32, 4)]):
        ys = [med([float(r["total_s"]) for r in mr_rows(s) if int(r["maps_req"]) == m and int(r["reducers"]) == k]) for s in sizes]
        if all(y == y for y in ys):
            ax.plot(xs, ys, marker="^", color=ps.color(1), alpha=1 - 0.3 * i, label=f"MapReduce m={m} r={k}")
    ax.set_xscale("log"); ax.set_yscale("log")
    ax.set_xlabel("records"); ax.set_ylabel("wall time (s)"); ax.set_title("Time vs input size — sequential, MPI, MapReduce")
    ax.legend(loc="center left", bbox_to_anchor=(1.01, 0.5), fontsize=8)
    ps.finish(fig, os.path.join(FIG, "wx_time_vs_size.png"), "MapReduce = ~31 s fixed (two jobs' start-up and commit) + a term that grows with input; MPI has no fixed term and scales with P.")

# ---- Fig 2: scaling with tasks ------------------------------------------------------
if sizes:
    fig, axes = plt.subplots(1, 2, figsize=(10, 3.8))
    for i, s in enumerate(sizes):
        ys = [med([float(r["wall_s"]) for r in mpi if r["size"] == s and int(r["P"]) == p]) for p in Ps]
        axes[0].plot(Ps, ys, marker="o", color=ps.color(i), label=s)
        rows = mr_rows(s); maps = sorted({int(r["maps_req"]) for r in rows})
        if maps:
            k = min({int(r["reducers"]) for r in rows})
            axes[1].plot(maps, [med([float(r["total_s"]) for r in rows if int(r["maps_req"]) == m and int(r["reducers"]) == k]) for m in maps],
                         marker="^", color=ps.color(i), label=f"{s} (r={k})")
    for ax, t, xl in [(axes[0], "MPI: wall time vs processes (log)", "P (MPI ranks)"), (axes[1], "MapReduce: job time vs mappers", "map tasks")]:
        ax.set_xscale("log", base=2); ax.set_title(t); ax.set_xlabel(xl); ax.set_ylabel("seconds"); ax.legend(fontsize=8)
    axes[0].set_yscale("log"); axes[1].set_ylim(bottom=0)
    ps.finish(fig, os.path.join(FIG, "wx_scaling.png"))

# ---- Fig 3: stage breakdown for MR on the largest size ----------------------------------
big = sizes[-1] if sizes else None
if big:
    rows = mr_rows(big); maps = sorted({int(r["maps_req"]) for r in rows}); k = min({int(r["reducers"]) for r in rows})
    s1 = [med([float(r["stage1_s"]) for r in rows if int(r["maps_req"]) == m and int(r["reducers"]) == k]) for m in maps]
    fn = [med([float(r["final_s"]) for r in rows if int(r["maps_req"]) == m and int(r["reducers"]) == k]) for m in maps]
    fig, ax = plt.subplots(figsize=(7, 3.6))
    x = range(len(maps))
    ax.bar(x, s1, color=ps.color(0), width=0.6, label="stage 1 (parse + in-mapper combine + merge)")
    ax.bar(x, fn, bottom=[a + 0.3 for a in s1], color=ps.color(1), width=0.6, label="final stage (1 reducer, report)")
    ax.set_xticks(list(x)); ax.set_xticklabels([f"m={m}" for m in maps]); ax.set_ylabel("seconds"); ax.legend()
    ax.set_title(f"Where the MapReduce time goes — {big} ({int(NREC[big]):,} records)")
    ps.finish(fig, os.path.join(FIG, "wx_stages.png"), "The final stage is constant (~15 s): a one-container job over a few thousand lines — pure framework overhead.")

# ---- Fig 4: speed-up vs sequential --------------------------------------------------
if sizes and mpi:
    fig, ax = plt.subplots(figsize=(9, 4.2))
    for i, s in enumerate(sizes):
        ax.plot(Ps, [seq_t[s] / med([float(r["wall_s"]) for r in mpi if r["size"] == s and int(r["P"]) == p]) for p in Ps], marker="o", color=ps.color(i), label=f"MPI {s}")
        rows = mr_rows(s); maps = sorted({int(r["maps_req"]) for r in rows})
        if maps:
            k = min({int(r["reducers"]) for r in rows})
            ax.plot(maps, [seq_t[s] / med([float(r["total_s"]) for r in rows if int(r["maps_req"]) == m and int(r["reducers"]) == k]) for m in maps], marker="^", linestyle="--", color=ps.color(i), label=f"MR {s}")
    ax.set_yscale("log"); ax.axhline(1, color=ps.TEXT2, linewidth=1, linestyle=":"); ax.set_xscale("log", base=2); ax.set_xlabel("MPI ranks (solid) / map tasks (dashed)"); ax.set_ylabel("speed-up over sequential (log)"); ax.set_title("Speed-up over the sequential program"); ax.legend(loc="center left", bbox_to_anchor=(1.01, 0.5), fontsize=8)
    ps.finish(fig, os.path.join(FIG, "wx_speedup.png"), "Below the line at 1 the parallel program is slower than running weather_seq on one core.")
