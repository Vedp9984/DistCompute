#!/usr/bin/env python3
"""Functional test of every RPC and every error path against a running server.

    python3 test_functional.py <server-host:port>
"""
import sys
import threading
import time

import grpc

import document_pb2 as pb
import document_pb2_grpc as pb_grpc

addr = sys.argv[1] if len(sys.argv) > 1 else "localhost:50051"
stub = pb_grpc.DocumentServiceStub(grpc.insecure_channel(addr))
doc = f"test_{int(time.time()*1000)}.txt"
fails = 0


def expect_code(code, fn, *a):
    global fails
    try:
        fn(*a)
        print(f"  [FAIL] expected {code.name}, call succeeded")
        fails += 1
    except grpc.RpcError as e:
        ok = e.code() == code
        fails += 0 if ok else 1
        print(f"  [{'ok' if ok else 'FAIL'}] {code.name}: {e.details()}")


def check(cond, msg):
    global fails
    fails += 0 if cond else 1
    print(f"  [{'ok' if cond else 'FAIL'}] {msg}")


print("== create / get ==")
r = stub.CreateDocument(pb.CreateDocumentRequest(name=doc, initial_content="Hello World"))
check(r.message == f"Document {doc} created.", "create message")
g = stub.GetDocument(pb.GetDocumentRequest(name=doc))
check(g.content == "Hello World" and g.version == 0, "get returns initial content, version 0")

print("== error paths ==")
expect_code(grpc.StatusCode.ALREADY_EXISTS, stub.CreateDocument,
            pb.CreateDocumentRequest(name=doc, initial_content="x"))
expect_code(grpc.StatusCode.INVALID_ARGUMENT, stub.CreateDocument,
            pb.CreateDocumentRequest(name="   ", initial_content="x"))
expect_code(grpc.StatusCode.NOT_FOUND, stub.GetDocument, pb.GetDocumentRequest(name="nope.txt"))
expect_code(grpc.StatusCode.NOT_FOUND, stub.EditDocument,
            pb.EditDocumentRequest(name="nope.txt", position=0, text="x"))
expect_code(grpc.StatusCode.OUT_OF_RANGE, stub.EditDocument,
            pb.EditDocumentRequest(name=doc, position=99, text="x"))
expect_code(grpc.StatusCode.NOT_FOUND,
            lambda req: next(iter(stub.SubscribeToUpdates(req))), pb.UpdateRequest(name="nope.txt"))

print("== subscribe + edit ==")
got = []
stream = stub.SubscribeToUpdates(pb.UpdateRequest(name=doc, client_id="tester"))
snap = []
def _pump():
    try:
        for u in stream:
            (snap if u.snapshot else got).append(u)
    except grpc.RpcError:
        pass  # CANCELLED when we unsubscribe below
threading.Thread(target=_pump, daemon=True).start()
time.sleep(0.3)
check(len(snap) == 1 and snap[0].content == "Hello World" and snap[0].version == 0, "subscription starts with a snapshot")
e = stub.EditDocument(pb.EditDocumentRequest(name=doc, position=6, text="Distributed ", client_id="c1"))
check(e.content == "Hello Distributed World", f"insert at 6 -> {e.content!r}")
e = stub.EditDocument(pb.EditDocumentRequest(name=doc, position=0, text=">> ", client_id="c1"))
check(e.content == ">> Hello Distributed World", "insert at 0 (prepend)")
n = len(e.content)
e = stub.EditDocument(pb.EditDocumentRequest(name=doc, position=n, text="!", client_id="c1"))
check(e.content == ">> Hello Distributed World!", "insert at len (append)")
e = stub.EditDocument(pb.EditDocumentRequest(name=doc, position=3, text="", client_id="c1"))
check(e.version == 4, "empty insert still bumps version")
time.sleep(0.5)
check(len(got) == 4, f"subscriber got {len(got)} updates")
check([u.version for u in got] == [1, 2, 3, 4], "in order")
check(got[-1].content == ">> Hello Distributed World!", "last update content matches")
stream.cancel()
time.sleep(0.3)
e = stub.EditDocument(pb.EditDocumentRequest(name=doc, position=0, text="x", client_id="c1"))
time.sleep(0.3)
check(len(got) == 4, "no updates after unsubscribe")

print("RESULT:", "PASS" if fails == 0 else f"FAIL ({fails})")
sys.exit(1 if fails else 0)
