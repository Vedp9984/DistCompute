#!/usr/bin/env python3
"""Figures + markdown tables for the SSSP benchmark.
    python3 plot.py [results_dir]      (default results/cluster)
"""
import csv
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "tools"))
import plotstyle as ps  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402

R = sys.argv[1] if len(sys.argv) > 1 else "results/cluster"
FIG = os.path.join(R, "fig"); os.makedirs(FIG, exist_ok=True)
GRAPHS = {"sample": (4, 4), "rnd1k": (1000, 5000), "rnd10k": (10000, 50000), "rnd10k_dense": (10000, 500000),
          "rnd10k_unreach": (10000, 30000), "layered10k": (10000, 40000), "star10k": (10000, 10000), "chain40": (40, 39)}

def load(name, headerless=None):
    rows = []
    with open(os.path.join(R, name)) as fh:
        first = fh.readline()
        cols = first.strip().split(",")
        if headerless and cols[0] != headerless[0]:
            cols = headerless; fh.seek(0)
        for line in fh:
            f = line.rstrip("\n").split(",")
            if len(f) >= len(cols) - 2:
                rows.append(dict(zip(cols, f)))
    return rows

runs = [r for r in load("runs.csv") if r["tag"] in GRAPHS]
iters = [r for r in load("iterations.csv", ["run", "tag", "mode", "reducers", "maps", "stage", "iter", "secs", "updated", "map_output_records"]) if r["tag"] in GRAPHS]
by_run = defaultdict(list)
for r in iters:
    by_run[r["run"]].append(r)

def find(tag, mode="frontier", red="2", maps="2"):
    c = [r for r in runs if r["tag"] == tag and r["mode"] == mode and r["reducers"] == red and r["maps"] == maps]
    return c[-1] if c else None

md = ["# SSSP on Hadoop — results\n", "## Runs\n",
      "| graph | V | E | mode | reducers | maps | iterations | total s | s / iteration |", "|---|---|---|---|---|---|---|---|---|"]
for r in sorted(runs, key=lambda r: (GRAPHS[r["tag"]][1], r["mode"], int(r["reducers"]), int(r["maps"]))):
    V, E = GRAPHS[r["tag"]]; it = int(r["iterations"]); tot = float(r["total_s"])
    md.append(f"| {r['tag']} | {V} | {E} | {r['mode']} | {r['reducers']} | {r['maps']} | {it} | {tot:.1f} | {tot/(it+2):.1f} |")

# ---- Fig 1: per-iteration profile of the densest graph (frontier) --------
r = find("rnd10k_dense") or find("rnd10k") or find("rnd1k")
if r:
    its = [x for x in by_run[r["run"]] if x["stage"] == "iter"]
    ks = [int(x["iter"]) for x in its]
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(10, 3.6))
    a1.bar(ks, [int(x["updated"]) for x in its], color=ps.color(0), width=0.7)
    a1.set_title(f"Vertices updated per iteration — {r['tag']}"); a1.set_xlabel("iteration"); a1.set_ylabel("UPDATED counter")
    a2.plot(ks, [float(x["secs"]) for x in its], marker="o", color=ps.color(1))
    a2.set_ylim(bottom=0); a2.set_title("Wall time per iteration (one job each)"); a2.set_xlabel("iteration"); a2.set_ylabel("seconds")
    ps.finish(fig, os.path.join(FIG, "sssp_iterations.png"), "The last iteration always reports UPDATED=0: that is the convergence check.")

# ---- Fig 2: total time vs graph size ------------------------------------
sz = [(t, find(t)) for t in ["sample", "rnd1k", "rnd10k", "rnd10k_dense"]]
sz = [(t, r) for t, r in sz if r]
if len(sz) >= 2:
    fig, ax = plt.subplots(figsize=(7, 3.4))
    xs = range(len(sz))
    ax.bar(xs, [float(r["total_s"]) for _, r in sz], color=ps.color(0), width=0.6)
    for x, (t, r) in zip(xs, sz):
        ax.text(x, float(r["total_s"]) + 5, f"{r['iterations']} iter", ha="center", fontsize=9, color=ps.TEXT2)
    ax.set_xticks(list(xs)); ax.set_xticklabels([f"{t}\nV={GRAPHS[t][0]} E={GRAPHS[t][1]}" for t, _ in sz])
    ax.set_ylabel("total wall time (s)"); ax.set_title("Total time vs graph size (2 reducers, 2 maps, frontier)")
    ps.finish(fig, os.path.join(FIG, "sssp_size.png"), "Time ≈ (iterations + 2) × ~18 s of per-job overhead; the graph size barely matters at this scale.")

# ---- Fig 3: reducers -----------------------------------------------------
rr = [(k, find("rnd10k_dense", red=k, maps="4")) for k in ["1", "4", "8"]]
rr = [(k, r) for k, r in rr if r]
r2 = find("rnd10k_dense", red="2", maps="2")
if len(rr) >= 2:
    fig, ax = plt.subplots(figsize=(6, 3.4))
    labels = [f"r={k}" for k, _ in rr]; vals = [float(r["total_s"]) / (int(r["iterations"]) + 2) for _, r in rr]
    ax.bar(range(len(rr)), vals, color=ps.color(0), width=0.6)
    ax.set_xticks(range(len(rr))); ax.set_xticklabels(labels)
    ax.set_ylabel("seconds per job"); ax.set_title("Reducer count — rnd10k_dense (4 maps)")
    ps.finish(fig, os.path.join(FIG, "sssp_reducers.png"))

# ---- Fig 4: frontier vs full ----------------------------------------------
for tag in ["rnd10k_dense", "rnd10k"]:
    fr, fu = find(tag, "frontier", "4", "4") or find(tag, "frontier", "2", "2"), find(tag, "full", "4", "4") or find(tag, "full", "2", "2")
    if fr and fu:
        fig, (a1, a2) = plt.subplots(1, 2, figsize=(10, 3.6))
        for i, (lab, r) in enumerate([("frontier (only changed vertices push)", fr), ("full (every reached vertex pushes)", fu)]):
            its = [x for x in by_run[r["run"]] if x["stage"] == "iter"]
            a1.plot([int(x["iter"]) for x in its], [int(x["map_output_records"]) for x in its], marker="o", color=ps.color(i), label=lab)
            a2.plot([int(x["iter"]) for x in its], [float(x["secs"]) for x in its], marker="o", color=ps.color(i), label=lab)
        a1.set_title(f"Shuffle volume per iteration — {tag}"); a1.set_xlabel("iteration"); a1.set_ylabel("map output records"); a1.legend(loc="center right", fontsize=8)
        a2.set_title("Wall time per iteration"); a2.set_xlabel("iteration"); a2.set_ylabel("seconds"); a2.set_ylim(bottom=0); a2.legend(loc="lower center", fontsize=8)
        ps.finish(fig, os.path.join(FIG, f"sssp_frontier_vs_full_{tag}.png"), "Same 19 rounds either way; the frontier variant moves 50× less data by the end, but at this scale a round costs the same ~16 s of framework overhead regardless.")
        break

# ---- Fig 5: shape → iterations ---------------------------------------------
sh = [(t, find(t)) for t in ["star10k", "rnd10k", "layered10k", "rnd10k_unreach", "chain40"]]
sh = [(t, r) for t, r in sh if r]
if len(sh) >= 2:
    fig, ax = plt.subplots(figsize=(7, 3.4))
    ax.bar(range(len(sh)), [int(r["iterations"]) for _, r in sh], color=ps.color(0), width=0.6)
    ax.set_xticks(range(len(sh))); ax.set_xticklabels([t for t, _ in sh])
    ax.set_ylabel("iterations to converge"); ax.set_title("Graph shape decides the iteration count (≈ hop-diameter from node 0 + 1)")
    ps.finish(fig, os.path.join(FIG, "sssp_shape.png"))

with open(os.path.join(R, "tables.md"), "w") as fh:
    fh.write("\n".join(md) + "\n")
print("wrote", os.path.join(R, "tables.md"))
