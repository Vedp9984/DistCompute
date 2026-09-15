#!/usr/bin/env python3
"""Streaming client: replays a dataset file as a live record stream.

    python3 streamer.py <coordinator host:port> DATASET [--batch 1000] [--rate 0]
                        [--limit N] [--csv results/stream.csv --tag NAME]

--batch  records per message (the message granularity under study)
--rate   target records/second; 0 = as fast as the pipeline accepts
--limit  send only the first N records (quick tests)
--preload  parse the whole file into batches *before* starting the clock, so
         the measured rate is the gRPC pipeline's, not the text parser's.
         Without it the streamer parses as it goes, like a real source would.

Prints one summary line: records, batches, wall time, records/s as seen by the
streamer and as reported by the coordinator.
"""
import argparse
import csv
import os
import sys
import time

import grpc

import common
import weather_pb2 as pb
import weather_pb2_grpc as pb_grpc


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("address")
    ap.add_argument("dataset")
    ap.add_argument("--batch", type=int, default=1000)
    ap.add_argument("--rate", type=float, default=0.0)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--csv", default="")
    ap.add_argument("--tag", default="")
    ap.add_argument("--preload", action="store_true")
    a = ap.parse_args()

    channel = grpc.insecure_channel(a.address, options=[("grpc.max_send_message_length", 256 << 20)])
    stub = pb_grpc.IngestServiceStub(channel)
    stats = {"records": 0, "batches": 0, "t0": None, "t1": None}

    fh = open(a.dataset)
    header = common.read_header(fh)
    if a.preload:
        # Parse everything before the RPC is even opened: the session (and the
        # coordinator's stream_active flag) must not exist while we are still
        # parsing, or "queries during ingest" would be measured against an idle
        # system with empty tables.
        t = time.perf_counter()
        pre = list(common.iter_batches(fh, a.batch, a.limit))
        print(f"preloaded {len(pre)} batches in {time.perf_counter()-t:.2f}s", file=sys.stderr)
        source = iter(pre)
    else:
        source = common.iter_batches(fh, a.batch, a.limit)

    def gen():
        yield pb.IngestMessage(header=header)
        stats["t0"] = time.perf_counter()
        for b in source:
            if a.rate > 0:
                # token bucket: don't get ahead of rate * elapsed
                due = stats["t0"] + stats["records"] / a.rate
                now = time.perf_counter()
                if due > now:
                    time.sleep(due - now)
            yield pb.IngestMessage(batch=b)
            stats["records"] += len(b.timestamp)
            stats["batches"] += 1
        stats["t1"] = time.perf_counter()
        fh.close()

    summary = stub.StreamRecords(gen())
    t_end = time.perf_counter()
    wall = t_end - stats["t0"]
    print(f"streamed {stats['records']} records in {stats['batches']} batches of {a.batch}: "
          f"wall {wall:.2f}s -> {stats['records']/wall:,.0f} rec/s (streamer)  "
          f"{summary.records_per_second:,.0f} rec/s (coordinator, {summary.elapsed_seconds:.2f}s)")
    if a.csv:
        new = not os.path.exists(a.csv)
        with open(a.csv, "a", newline="") as fh:
            w = csv.writer(fh)
            if new:
                w.writerow(["tag", "dataset", "batch", "rate", "preload", "records", "batches", "wall_s",
                            "streamer_rps", "coordinator_rps", "coordinator_elapsed_s"])
            w.writerow([a.tag, os.path.basename(a.dataset), a.batch, a.rate, int(a.preload), stats["records"],
                        stats["batches"], f"{wall:.4f}", f"{stats['records']/wall:.1f}",
                        f"{summary.records_per_second:.1f}", f"{summary.elapsed_seconds:.4f}"])


if __name__ == "__main__":
    main()
