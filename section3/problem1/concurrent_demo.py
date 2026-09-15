#!/usr/bin/env python3
"""Concurrent-edit demonstration (PDF section 1.5, step 5).

    python3 concurrent_demo.py <server-host:port> [--clients N] [--doc NAME]

Fires N EditDocument RPCs at the *same position* from N threads that are
released simultaneously by a barrier, while a subscriber records every update
the server pushes.  Afterwards it checks that:

  * every edit was applied exactly once (final length == initial + sum of inserts),
  * the version counter advanced by exactly N,
  * the subscriber saw exactly N updates, in version order, and the last one
    equals the final document,
  * no inserted string was torn (each appears contiguously in the result).
"""
import argparse
import threading
import time

import grpc

import document_pb2 as pb
import document_pb2_grpc as pb_grpc


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("address")
    ap.add_argument("--clients", type=int, default=2)
    ap.add_argument("--doc", default=f"concurrent_{int(time.time())}.txt")
    args = ap.parse_args()

    initial = "Hello World"
    position = 6
    inserts = ["Distributed ", "Systems "] + [f"Client{i} " for i in range(2, args.clients)]
    inserts = inserts[: args.clients]

    channel = grpc.insecure_channel(args.address)
    stub = pb_grpc.DocumentServiceStub(channel)
    stub.CreateDocument(pb.CreateDocumentRequest(name=args.doc, initial_content=initial))
    print(f"[demo] created {args.doc}: {initial!r}")

    # Subscriber on its own channel, started before the edits.
    updates = []
    sub_ready = threading.Event()

    def subscriber():
        s = pb_grpc.DocumentServiceStub(grpc.insecure_channel(args.address))
        stream = s.SubscribeToUpdates(pb.UpdateRequest(name=args.doc, client_id="observer"))
        for u in stream:
            if u.snapshot:            # the server confirms the subscription is live
                sub_ready.set()
                continue
            updates.append(u)
            print(f"[Update] v{u.version} by {u.edited_by:<9} -> {u.content!r}")
            if len(updates) == len(inserts):
                stream.cancel()
                break

    threading.Thread(target=subscriber, daemon=True).start()
    sub_ready.wait()   # blocks until the server has sent the subscription snapshot

    barrier = threading.Barrier(len(inserts))
    results = {}

    def editor(i, text):
        s = pb_grpc.DocumentServiceStub(grpc.insecure_channel(args.address))
        barrier.wait()  # all editors leave the gate together
        t0 = time.perf_counter()
        r = s.EditDocument(pb.EditDocumentRequest(name=args.doc, position=position,
                                                  text=text, client_id=f"client{i+1}"))
        results[i] = (r.version, r.content, time.perf_counter() - t0)

    threads = [threading.Thread(target=editor, args=(i, t)) for i, t in enumerate(inserts)]
    print(f"[demo] {len(inserts)} clients inserting at position {position} simultaneously ...")
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    for i in sorted(results):
        v, c, dt = results[i]
        print(f"[client{i+1}] inserted {inserts[i]!r} -> got version {v} ({dt*1e3:.1f} ms): {c!r}")

    final = stub.GetDocument(pb.GetDocumentRequest(name=args.doc))
    time.sleep(0.5)
    print(f"[demo] final document (v{final.version}): {final.content!r}")

    # ---- checks ----
    ok = True
    exp_len = len(initial) + sum(len(t) for t in inserts)
    ok &= _check(len(final.content) == exp_len, f"length {len(final.content)} == {exp_len}")
    ok &= _check(final.version == len(inserts), f"version {final.version} == {len(inserts)}")
    ok &= _check(all(t in final.content for t in inserts), "every insert present and untorn")
    ok &= _check(sorted(v for v, _, _ in results.values()) == list(range(1, len(inserts) + 1)),
                 "each edit got a distinct consecutive version")
    ok &= _check(len(updates) == len(inserts), f"subscriber received {len(updates)} updates")
    ok &= _check([u.version for u in updates] == list(range(1, len(inserts) + 1)),
                 "updates arrived in version order")
    ok &= _check(updates and updates[-1].content == final.content,
                 "last update equals final document")
    print("[demo] RESULT:", "PASS" if ok else "FAIL")
    raise SystemExit(0 if ok else 1)


def _check(cond, msg):
    print(f"  [{'ok' if cond else 'FAIL'}] {msg}")
    return bool(cond)


if __name__ == "__main__":
    main()
