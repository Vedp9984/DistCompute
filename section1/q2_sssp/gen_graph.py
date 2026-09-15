#!/usr/bin/env python3
"""Reproducible weighted directed graph generator.

    python3 gen_graph.py V E [--seed S] [--wmax 1000] [--shape random|chain|layered|star]
                                        [--unreachable FRAC] > graph.txt

random    : E distinct random edges (multi-edges/self-loops skipped and redrawn)
chain     : 0->1->2->...->V-1 plus random extras  -> long diameter, many iterations
layered   : vertices in ceil(sqrt V) layers, edges only forward one layer  -> medium diameter
star      : 0 -> everyone, plus random extras       -> diameter 1, converges in 2 iterations
--unreachable FRAC : the last FRAC*V vertices receive no in-edges (must print INF)
"""
import argparse, math, random, sys

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("V", type=int); ap.add_argument("E", type=int)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--wmax", type=int, default=1000)
    ap.add_argument("--shape", default="random", choices=["random", "chain", "layered", "star"])
    ap.add_argument("--unreachable", type=float, default=0.0)
    a = ap.parse_args()
    rng = random.Random(a.seed)
    V, E = a.V, a.E
    reach_V = V - int(a.unreachable * V)   # only vertices < reach_V may be edge targets
    reach_V = max(reach_V, 1)
    edges = set()
    def add(u, v):
        if u != v and v < reach_V and (u, v) not in edges:
            edges.add((u, v)); return True
        return False
    if a.shape == "chain":
        for i in range(min(reach_V, V) - 1): add(i, i + 1)
    elif a.shape == "star":
        for i in range(1, reach_V): add(0, i)
    elif a.shape == "layered":
        L = max(2, math.isqrt(reach_V))
        layer = lambda x: min(x * L // reach_V, L - 1)
        by_layer = {}
        for x in range(reach_V): by_layer.setdefault(layer(x), []).append(x)
        for l in range(L - 1):
            for x in by_layer[l]:
                add(x, rng.choice(by_layer[l + 1]))
    tries = 0
    while len(edges) < E and tries < 50 * E:
        tries += 1
        u = rng.randrange(V)
        if a.shape == "layered":
            l = min(u * L // reach_V, L - 1)
            if l + 1 >= L or u >= reach_V: continue
            v = rng.choice(by_layer[l + 1])
        else:
            v = rng.randrange(reach_V)
        add(u, v)
    edges = sorted(edges)
    rng.shuffle(edges)
    out = sys.stdout
    out.write(f"{V} {len(edges)}\n")
    for u, v in edges:
        out.write(f"{u} {v} {rng.randint(1, a.wmax)}\n")

if __name__ == "__main__":
    main()
