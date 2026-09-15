#!/usr/bin/env python3
"""Analytics worker.

    python3 worker.py [--port 50061] [--id w0]

Holds one ``Partial`` (see common.py).  The coordinator streams columnar
batches into ``Process``; the dashboard's queries reach us as ``GetPartial``.
One lock serialises batch application and snapshotting, so a query never sees
a half-applied batch.  Snapshotting the station/interval tables is O(S + R) and
happens under the lock -- with S <= 10 000 and R ~ 20 000 that is a
millisecond or two, which is the price of a consistent answer.
"""
import argparse
import logging
import os
import resource
import threading
import time
from concurrent import futures

import grpc

import common
import weather_pb2 as pb
import weather_pb2_grpc as pb_grpc

log = logging.getLogger("worker")


class Worker(pb_grpc.WorkerServiceServicer):
    def __init__(self, worker_id: str):
        self.id = worker_id
        self.lock = threading.Lock()
        self.state = common.Partial()
        self.records = 0
        self.t0 = time.process_time()

    def Process(self, request_iterator, context):
        n_rec = n_batch = 0
        for batch in request_iterator:
            with self.lock:
                n = self.state.add_batch(batch)
            n_rec += n
            n_batch += 1
        with self.lock:
            self.records += n_rec
        log.info("session done: %d records in %d batches (total %d)", n_rec, n_batch, self.records)
        return pb.WorkerAck(records_processed=n_rec, batches_processed=n_batch)

    def GetPartial(self, request, context):
        with self.lock:
            return self.state.to_pb(self.id)

    def GetInfo(self, request, context):
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024
        with self.lock:
            rec = self.state.count
        return pb.WorkerInfo(worker_id=self.id, records_processed=rec,
                             cpu_seconds=time.process_time() - self.t0, rss_bytes=rss)

    def Reset(self, request, context):
        with self.lock:
            self.state = common.Partial()
            self.records = 0
        log.info("state reset")
        return pb.Empty()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=50061)
    ap.add_argument("--id", default=None)
    ap.add_argument("--bind", default="0.0.0.0")
    a = ap.parse_args()
    wid = a.id or f"{os.uname().nodename}:{a.port}"
    logging.basicConfig(level=logging.INFO, format=f"%(asctime)s [worker {wid}] %(message)s", datefmt="%H:%M:%S")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8),
                         options=[("grpc.max_receive_message_length", 256 << 20)])
    pb_grpc.add_WorkerServiceServicer_to_server(Worker(wid), server)
    server.add_insecure_port(f"{a.bind}:{a.port}")
    server.start()
    log.info("listening on %s:%d", a.bind, a.port)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(1)


if __name__ == "__main__":
    main()
