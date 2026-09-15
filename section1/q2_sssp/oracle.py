#!/usr/bin/env python3
"""Reference SSSP (Dijkstra) for the assignment's input format.  stdin -> stdout."""
import heapq, sys

def main():
    data = sys.stdin.read().split()
    V, E = int(data[0]), int(data[1])
    adj = [[] for _ in range(V)]
    for i in range(E):
        u, v, w = int(data[2 + 3*i]), int(data[3 + 3*i]), int(data[4 + 3*i])
        adj[u].append((v, w))
    INF = float("inf")
    dist = [INF] * V
    dist[0] = 0
    pq = [(0, 0)]
    while pq:
        d, u = heapq.heappop(pq)
        if d > dist[u]:
            continue
        for v, w in adj[u]:
            if d + w < dist[v]:
                dist[v] = d + w
                heapq.heappush(pq, (dist[v], v))
    out = sys.stdout
    for i in range(V):
        out.write(f"{i} {'INF' if dist[i] == INF else dist[i]}\n")

if __name__ == "__main__":
    main()
