#!/usr/bin/env python3
"""Coordinator: receives the record stream, fans it out to workers, answers queries.

    python3 coordinator.py --workers host:port,host:port,... [--port 50051]
                           [--dist rr|station] [--queue 64]

Distribution strategies
  rr       whole batches round-robin over workers.  Perfect load balance, no
           per-record work in the coordinator; every worker sees every station,
           so each worker's station table is the full S and queries merge W
           tables of size S.
  station  records re-bucketed by station_id % W.  Each station lives on exactly
           one worker, so the per-station tables are disjoint (merge is a
           concatenation) and the top-K could in principle be pushed down.
           Costs a Python-level re-partition of every batch in the coordinator.

Each worker gets one long-lived ``Process`` stream per ingest session, fed from
a bounded queue by a sender thread.  The bounded queue is the back-pressure:
if workers fall behind, the coordinator's ingest loop blocks, and gRPC flow
control in turn slows the streamer.  Queries (``GetAnalytics``) never touch the
ingest path -- they fan out ``GetPartial`` to all workers in parallel and merge,
so they are answered while ingestion is running.
"""
import argparse
import logging
import queue
import resource
import threading
import time
from concurrent import futures

import grpc

import common
import weather_pb2 as pb
import weather_pb2_grpc as pb_grpc

log = logging.getLogger("coord")
SENTINEL = None


class WorkerLink:
    def __init__(self, addr: str, qsize: int):
        self.addr = addr
        self.channel = grpc.insecure_channel(addr, options=[
            ("grpc.max_send_message_length", 256 << 20),
            ("grpc.max_receive_message_length", 256 << 20)])
        self.stub = pb_grpc.WorkerServiceStub(self.channel)
        self.q: queue.Queue = queue.Queue(maxsize=qsize)
        self.future = None
        self.sent = 0

    def _gen(self):
        while True:
            item = self.q.get()
            if item is SENTINEL:
                return
            yield item

    def open(self):
        self.future = self.stub.Process.future(self._gen())

    def send(self, batch):
        self.q.put(batch)
        self.sent += len(batch.timestamp)

    def close(self) -> pb.WorkerAck:
        self.q.put(SENTINEL)
        return self.future.result()


class Coordinator(pb_grpc.IngestServiceServicer, pb_grpc.QueryServiceServicer):
    def __init__(self, workers, dist: str, qsize: int):
        self.links = [WorkerLink(w, qsize) for w in workers]
        self.dist = dist
        self.pool = futures.ThreadPoolExecutor(max_workers=max(4, len(self.links)))
        self.lock = threading.Lock()
        self.header = pb.Header()
        self.records_ingested = 0
        self.batches = 0
        self.active = False
        self.complete = False
        self.t_first = 0.0
        self.t_last = 0.0
        self.rate_window = []      # (t, cumulative records)
        self.query_count = 0
        for l in self.links:
            grpc.channel_ready_future(l.channel).result(timeout=30)
        log.info("connected to %d workers: %s (dist=%s)", len(self.links), workers, dist)

    # -- ingest ---------------------------------------------------------------
    def _dispatch_rr(self, batch, i):
        self.links[i % len(self.links)].send(batch)

    def _dispatch_station(self, batch, i):
        W = len(self.links)
        if W == 1:
            self.links[0].send(batch)
            return
        parts = [pb.RecordBatch(seq=batch.seq) for _ in range(W)]
        idx = [[] for _ in range(W)]
        for j, s in enumerate(batch.station_id):
            idx[s % W].append(j)
        cols = ("timestamp", "station_id", "temperature", "humidity", "pressure", "rainfall", "wind_speed")
        src = {c: getattr(batch, c) for c in cols}
        for w in range(W):
            if not idx[w]:
                continue
            sel = idx[w]
            p = parts[w]
            for c in cols:
                col = src[c]
                getattr(p, c).extend([col[j] for j in sel])
            self.links[w].send(p)

    def StreamRecords(self, request_iterator, context):
        with self.lock:
            if self.active:
                context.abort(grpc.StatusCode.FAILED_PRECONDITION, "an ingest session is already running")
            self.active = True
            self.complete = False
            self.records_ingested = 0
            self.batches = 0
            self.rate_window.clear()
            # a new session replaces the previous dataset
            for l in self.links:
                l.stub.Reset(pb.Empty())
                l.open()
        dispatch = self._dispatch_station if self.dist == "station" else self._dispatch_rr
        t_first = None
        i = 0
        for msg in request_iterator:
            if msg.HasField("header"):
                with self.lock:
                    self.header.CopyFrom(msg.header)
                log.info("header N=%d K=%d S=%d", msg.header.n, msg.header.k, msg.header.s)
                continue
            b = msg.batch
            if t_first is None:
                t_first = time.perf_counter()
                with self.lock:
                    self.t_first = t_first
            dispatch(b, i)
            i += 1
            n = len(b.timestamp)
            with self.lock:
                self.records_ingested += n
                self.batches += 1
                now = time.perf_counter()
                self.rate_window.append((now, self.records_ingested))
                if len(self.rate_window) > 4096:
                    del self.rate_window[:2048]
        acks = [l.close() for l in self.links]
        t_end = time.perf_counter()
        with self.lock:
            self.t_last = t_end
            self.active = False
            self.complete = True
            total = self.records_ingested
        elapsed = t_end - (t_first or t_end)
        log.info("session complete: %d records, %d batches, %.2fs, %.0f rec/s; worker acks=%s",
                 total, i, elapsed, total / elapsed if elapsed else 0.0,
                 [a.records_processed for a in acks])
        return pb.IngestSummary(records_received=total, batches_received=i, elapsed_seconds=elapsed,
                                records_per_second=total / elapsed if elapsed else 0.0)

    # -- query ----------------------------------------------------------------
    def _rate(self) -> float:
        w = self.rate_window
        if len(w) < 2:
            return 0.0
        t_now, r_now = w[-1]
        for t, r in reversed(w):
            if t_now - t >= 1.0:
                return (r_now - r) / (t_now - t)
        t0, r0 = w[0]
        return (r_now - r0) / (t_now - t0) if t_now > t0 else 0.0

    def GetAnalytics(self, request, context):
        t0 = time.perf_counter()
        parts = list(self.pool.map(lambda l: l.stub.GetPartial(pb.Empty()), self.links))
        merged = common.merge_all(common.Partial.from_pb(p) for p in parts)
        rep = pb.AnalyticsReport()
        with self.lock:
            k = request.top_k or self.header.k
            rep.header.CopyFrom(self.header)
            rep.stream_active = self.active
            rep.stream_complete = self.complete
            rep.records_ingested = self.records_ingested
            rep.ingest_rate = self._rate() if self.active else 0.0
            end = self.t_last if self.complete else time.perf_counter()
            rep.elapsed_seconds = (end - self.t_first) if self.t_first else 0.0
            self.query_count += 1
            rep.query_count = self.query_count
        common.fill_report(rep, merged, k)
        rep.records_processed = merged.count
        if request.include_workers:
            infos = list(self.pool.map(lambda l: l.stub.GetInfo(pb.Empty()), self.links))
            rep.workers.extend(infos)
        rep.coord_cpu_seconds = time.process_time()
        rep.coord_rss_bytes = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024
        rep.merge_ms = (time.perf_counter() - t0) * 1e3
        return rep


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=50051)
    ap.add_argument("--bind", default="0.0.0.0")
    ap.add_argument("--workers", required=True, help="comma-separated host:port list")
    ap.add_argument("--dist", choices=["rr", "station"], default="rr")
    ap.add_argument("--queue", type=int, default=64, help="per-worker send queue (batches)")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [coord] %(message)s", datefmt="%H:%M:%S")
    coord = Coordinator(a.workers.split(","), a.dist, a.queue)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=32), options=[
        ("grpc.max_receive_message_length", 256 << 20)])
    pb_grpc.add_IngestServiceServicer_to_server(coord, server)
    pb_grpc.add_QueryServiceServicer_to_server(coord, server)
    server.add_insecure_port(f"{a.bind}:{a.port}")
    server.start()
    log.info("listening on %s:%d", a.bind, a.port)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(1)


if __name__ == "__main__":
    main()
