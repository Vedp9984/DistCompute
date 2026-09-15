#!/usr/bin/env python3
"""CLI dashboard / query client.

    python3 dashboard.py <coordinator host:port>                # live view, refresh every 1 s
    python3 dashboard.py <addr> --interval 0.5                  # faster refresh
    python3 dashboard.py <addr> --once                          # one snapshot, no screen clearing
    python3 dashboard.py <addr> --final [--wait]                # exact HW2 report on stdout
                                                                #   (--wait: block until the stream completes)
    python3 dashboard.py <addr> --raw                           # dump the AnalyticsReport message

The live view shows the HW2 statistics as they currently stand, plus the
stream state (records ingested/processed, current ingest rate, elapsed time,
per-worker load) and the query's own round-trip latency.
"""
import argparse
import sys
import time

import grpc

import common
import weather_pb2 as pb
import weather_pb2_grpc as pb_grpc


def fmt_int(n):
    return f"{n:,}"


def render(r: pb.AnalyticsReport, latency_ms: float, addr: str) -> str:
    st = "STREAMING" if r.stream_active else ("COMPLETE" if r.stream_complete else "IDLE")
    hdr = r.header
    progress = ""
    if hdr.n:
        progress = f"  ({100.0 * r.records_ingested / hdr.n:5.1f}% of {fmt_int(hdr.n)})"
    lines = [
        f"┌─ Weather analytics dashboard ── {addr} ── {time.strftime('%H:%M:%S')} ─",
        f"│ stream: {st:<9} ingested {fmt_int(r.records_ingested):>12}{progress}   processed by workers {fmt_int(r.records_processed):>12}",
        f"│ rate:   {r.ingest_rate:>12,.0f} rec/s   elapsed {r.elapsed_seconds:8.2f}s   "
        f"query latency {latency_ms:7.1f} ms (merge {r.merge_ms:.1f} ms)   queries served {r.query_count}",
    ]
    if r.workers:
        parts = [f"{w.worker_id}: {fmt_int(w.records_processed)} rec, cpu {w.cpu_seconds:.1f}s, rss {w.rss_bytes/2**20:.0f}MB"
                 for w in r.workers]
        lines.append("│ workers: " + " | ".join(parts))
    lines.append("├" + "─" * 78)
    lines += [
        f"│ TOTAL_MEASUREMENTS          {fmt_int(r.total_measurements)}",
        f"│ TEMPERATURE  avg {r.avg_temperature:8.2f}   min {r.min_temperature:8.2f}   max {r.max_temperature:8.2f}",
        f"│ HUMIDITY     avg {r.avg_humidity:8.2f}   min {r.min_humidity:8.2f}   max {r.max_humidity:8.2f}",
        f"│ PRESSURE     avg {r.avg_pressure:8.2f}   min {r.min_pressure:8.2f}   max {r.max_pressure:8.2f}",
        f"│ RAINFALL   total {r.total_rainfall:8.2f}   max {r.max_rainfall:8.2f}",
        f"│ WIND         avg {r.avg_wind_speed:8.2f}   max {r.max_wind_speed:8.2f}",
        f"│ EXTREME_TEMPERATURE_EVENTS  {fmt_int(r.extreme_temperature_events)}",
        f"│ HOTTEST  {r.hottest.temp:7.2f} °C  station {r.hottest.station:<6} ts {r.hottest.timestamp}",
        f"│ COLDEST  {r.coldest.temp:7.2f} °C  station {r.coldest.station:<6} ts {r.coldest.timestamp}",
        f"│ BUSIEST_INTERVAL  bucket {r.busiest_interval} ({r.busiest_count} records)   "
        f"stations seen {r.stations_seen}   intervals seen {r.intervals_seen}",
        f"│ TOP_STATIONS (K={len(r.top_stations)})   id     count   avg_temp   total_rain",
    ]
    for s in r.top_stations:
        lines.append(f"│     {s.station:>22}  {s.count:>8}   {s.avg_temp:8.2f}   {s.total_rain:10.2f}")
    lines.append("└" + "─" * 78)
    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("address")
    ap.add_argument("--interval", type=float, default=1.0)
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--final", action="store_true", help="print the HW2-format report and exit")
    ap.add_argument("--wait", action="store_true", help="with --final: wait for stream completion")
    ap.add_argument("--raw", action="store_true")
    ap.add_argument("--k", type=int, default=0)
    a = ap.parse_args()
    stub = pb_grpc.QueryServiceStub(grpc.insecure_channel(a.address))
    req = pb.QueryRequest(top_k=a.k, include_workers=not a.final)

    if a.final:
        while True:
            r = stub.GetAnalytics(req)
            if r.stream_complete or not a.wait:
                break
            time.sleep(0.5)
        print("\n".join(common.report_lines(r)))
        return
    if a.raw:
        print(stub.GetAnalytics(req))
        return
    try:
        while True:
            t0 = time.perf_counter()
            r = stub.GetAnalytics(req)
            lat = (time.perf_counter() - t0) * 1e3
            out = render(r, lat, a.address)
            if a.once:
                print(out)
                return
            sys.stdout.write("\x1b[H\x1b[2J" + out + "\n")
            sys.stdout.flush()
            time.sleep(a.interval)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
