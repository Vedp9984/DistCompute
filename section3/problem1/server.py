#!/usr/bin/env python3
"""Collaborative document server.

    python3 server.py [host:port]        (default 0.0.0.0:50051)

Documents are kept in memory.  Every document carries its own lock, so edits to
different documents proceed in parallel while edits to the *same* document are
serialised: two clients inserting at position 6 at the same instant both
succeed, in whatever order the server happened to take the lock, and the second
one sees the first one's text already in place.  Subscribers are fed through
per-subscriber queues so a slow or dead subscriber never blocks an edit.
"""
import logging
import queue
import sys
import threading
from concurrent import futures

import grpc

import document_pb2 as pb
import document_pb2_grpc as pb_grpc

log = logging.getLogger("docserver")


class Document:
    def __init__(self, name: str, content: str):
        self.name = name
        self.content = content
        self.version = 0
        self.lock = threading.Lock()
        # One queue per active SubscribeToUpdates stream.
        self.subscribers: list[queue.Queue] = []

    # Called with self.lock held.  Puts the update on every subscriber queue;
    # the queues are unbounded so this never blocks the editing client.
    def _broadcast(self, update: pb.DocumentUpdate):
        for q in list(self.subscribers):
            q.put(update)


class DocumentService(pb_grpc.DocumentServiceServicer):
    def __init__(self):
        self.docs: dict[str, Document] = {}
        # Guards the dict itself (create / lookup).  Never held while a
        # per-document lock is held, so there is a strict lock order and no
        # possibility of deadlock.
        self.registry_lock = threading.Lock()

    # -- helpers ---------------------------------------------------------------
    def _get(self, name: str, context) -> Document:
        with self.registry_lock:
            doc = self.docs.get(name)
        if doc is None:
            context.abort(grpc.StatusCode.NOT_FOUND, f"document '{name}' does not exist")
        return doc

    # -- RPCs ------------------------------------------------------------------
    def CreateDocument(self, request, context):
        name = request.name.strip()
        if not name:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "document name must not be empty")
        with self.registry_lock:
            if name in self.docs:
                context.abort(grpc.StatusCode.ALREADY_EXISTS, f"document '{name}' already exists")
            self.docs[name] = Document(name, request.initial_content)
        log.info("created %-20s (%d chars) by %s", name, len(request.initial_content), context.peer())
        return pb.CreateDocumentResponse(name=name, message=f"Document {name} created.")

    def GetDocument(self, request, context):
        doc = self._get(request.name, context)
        with doc.lock:
            return pb.GetDocumentResponse(name=doc.name, content=doc.content, version=doc.version)

    def EditDocument(self, request, context):
        doc = self._get(request.name, context)
        with doc.lock:
            if request.position > len(doc.content):
                context.abort(
                    grpc.StatusCode.OUT_OF_RANGE,
                    f"position {request.position} is beyond the end of the document "
                    f"(length {len(doc.content)})",
                )
            pos = request.position
            doc.content = doc.content[:pos] + request.text + doc.content[pos:]
            doc.version += 1
            update = pb.DocumentUpdate(
                name=doc.name,
                content=doc.content,
                version=doc.version,
                edited_by=request.client_id or context.peer(),
                position=pos,
                inserted_text=request.text,
            )
            doc._broadcast(update)
            log.info("edit    %-20s v%-4d pos=%-4d +%-3d chars by %s -> %d subscriber(s)",
                     doc.name, doc.version, pos, len(request.text),
                     update.edited_by, len(doc.subscribers))
            return pb.EditDocumentResponse(name=doc.name, content=doc.content, version=doc.version)

    def SubscribeToUpdates(self, request, context):
        doc = self._get(request.name, context)
        q: queue.Queue = queue.Queue()
        with doc.lock:
            doc.subscribers.append(q)
            # Snapshot under the same lock, so no edit can slip between the
            # snapshot and the first real update.
            q.put(pb.DocumentUpdate(name=doc.name, content=doc.content, version=doc.version, snapshot=True))
        who = request.client_id or context.peer()
        log.info("subscribe %-18s by %s (now %d)", doc.name, who, len(doc.subscribers))

        def unsubscribe():
            with doc.lock:
                if q in doc.subscribers:
                    doc.subscribers.remove(q)
            log.info("unsubscribe %-16s by %s (now %d)", doc.name, who, len(doc.subscribers))

        # Fires when the client cancels the stream or the connection drops.
        context.add_callback(unsubscribe)
        try:
            while context.is_active():
                try:
                    yield q.get(timeout=1.0)   # wake periodically to re-check liveness
                except queue.Empty:
                    continue
        finally:
            unsubscribe()


def serve(address: str):
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=64),
        options=[("grpc.keepalive_time_ms", 20000)],
    )
    pb_grpc.add_DocumentServiceServicer_to_server(DocumentService(), server)
    if server.add_insecure_port(address) == 0:
        log.error("could not bind %s (port in use?)", address)
        sys.exit(1)
    server.start()
    log.info("DocumentService listening on %s", address)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        log.info("shutting down")
        server.stop(grace=2)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [server] %(message)s",
                        datefmt="%H:%M:%S")
    addr = sys.argv[1] if len(sys.argv) > 1 else "0.0.0.0:50051"
    # "localhost:50051" only accepts loopback; when clients are on other nodes
    # the server must bind 0.0.0.0 (the guide's example) or its hostname.
    serve(addr)
