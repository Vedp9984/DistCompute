#!/usr/bin/env python3
"""Collaborative document client.

    python3 client.py <server-host:port> [client-name]

Interactive CLI.  Either pick a menu number or type the command directly:

    1 / create <name> "<initial content>"
    2 / open <name>
    3 / edit <name> <position> "<text>"
    4 / subscribe <name>
    5 / exit
        unsubscribe <name>          (extra: stop a running subscription)

Subscriptions run in background threads, so `[Update]` lines arrive while the
prompt stays usable.
"""
import os
import shlex
import sys
import threading

import grpc

import document_pb2 as pb
import document_pb2_grpc as pb_grpc

MENU = """
==== Collaborative Document Client ====
  1. Create Document      create <name> "<content>"
  2. Open Document        open <name>
  3. Edit Document        edit <name> <pos> "<text>"
  4. Subscribe to Updates subscribe <name>
  5. Exit                 exit
"""


class Client:
    def __init__(self, address: str, name: str):
        self.name = name
        self.channel = grpc.insecure_channel(address)
        self.stub = pb_grpc.DocumentServiceStub(self.channel)
        self.subscriptions: dict[str, tuple[grpc.Future, threading.Thread]] = {}
        self.print_lock = threading.Lock()

    def out(self, *lines):
        with self.print_lock:
            for line in lines:
                print(line, flush=True)

    # -- operations ------------------------------------------------------------
    def create(self, name, content):
        r = self.stub.CreateDocument(pb.CreateDocumentRequest(name=name, initial_content=content))
        self.out(f"[Client] {r.message}")

    def open(self, name):
        r = self.stub.GetDocument(pb.GetDocumentRequest(name=name))
        self.out(f"[Client] {r.content}")

    def edit(self, name, position, text):
        r = self.stub.EditDocument(
            pb.EditDocumentRequest(name=name, position=position, text=text, client_id=self.name))
        self.out(f"[Client] Edit applied (version {r.version}).", f"[Client] {r.content}")

    def subscribe(self, name):
        if name in self.subscriptions:
            self.out(f"[Client] Already subscribed to {name}.")
            return
        # Validate up front so the user gets an immediate NOT_FOUND rather than
        # a silent stream that never yields.
        self.stub.GetDocument(pb.GetDocumentRequest(name=name))
        stream = self.stub.SubscribeToUpdates(pb.UpdateRequest(name=name, client_id=self.name))

        def pump():
            try:
                for upd in stream:
                    if upd.snapshot:
                        self.out(f"[Client] Subscribed to updates (current version {upd.version}).")
                        continue
                    self.out(f"\n[Update] Document {upd.name} modified "
                             f"(v{upd.version}, by {upd.edited_by}: inserted "
                             f"{upd.inserted_text!r} at {upd.position}).",
                             upd.content)
            except grpc.RpcError as e:
                if e.code() != grpc.StatusCode.CANCELLED:
                    self.out(f"[Client] Subscription to {name} ended: {e.code().name} {e.details()}")
            finally:
                self.subscriptions.pop(name, None)

        t = threading.Thread(target=pump, daemon=True)
        t.start()
        self.subscriptions[name] = (stream, t)

    def unsubscribe(self, name):
        entry = self.subscriptions.get(name)
        if not entry:
            self.out(f"[Client] Not subscribed to {name}.")
            return
        entry[0].cancel()
        self.out(f"[Client] Unsubscribed from {name}.")

    def close(self):
        for stream, _ in list(self.subscriptions.values()):
            stream.cancel()
        self.channel.close()

    # -- CLI -------------------------------------------------------------------
    def prompt(self, text):
        try:
            return input(text)
        except EOFError:
            return "exit"

    def dispatch(self, line: str) -> bool:
        try:
            parts = shlex.split(line)
        except ValueError as e:
            self.out(f"[Client] Parse error: {e}")
            return True
        if not parts:
            return True
        cmd, args = parts[0].lower(), parts[1:]

        # Menu numbers: prompt for the missing pieces.
        if cmd == "1":
            cmd, args = "create", [self.prompt("  name: "), self.prompt("  initial content: ")]
        elif cmd == "2":
            cmd, args = "open", [self.prompt("  name: ")]
        elif cmd == "3":
            cmd, args = "edit", [self.prompt("  name: "), self.prompt("  position: "),
                                 self.prompt("  text: ")]
        elif cmd == "4":
            cmd, args = "subscribe", [self.prompt("  name: ")]
        elif cmd == "5":
            cmd = "exit"

        try:
            if cmd == "create":
                if len(args) < 1:
                    raise ValueError('usage: create <name> "<content>"')
                self.create(args[0], " ".join(args[1:]))
            elif cmd == "open":
                if len(args) != 1:
                    raise ValueError("usage: open <name>")
                self.open(args[0])
            elif cmd == "edit":
                if len(args) < 2:
                    raise ValueError('usage: edit <name> <position> "<text>"')
                self.edit(args[0], int(args[1]), " ".join(args[2:]))
            elif cmd == "subscribe":
                if len(args) != 1:
                    raise ValueError("usage: subscribe <name>")
                self.subscribe(args[0])
            elif cmd == "unsubscribe":
                if len(args) != 1:
                    raise ValueError("usage: unsubscribe <name>")
                self.unsubscribe(args[0])
            elif cmd in ("exit", "quit", "q"):
                return False
            elif cmd in ("help", "?", "menu"):
                self.out(MENU)
            else:
                self.out(f"[Client] Unknown command '{cmd}'. Type 'help' for the menu.")
        except grpc.RpcError as e:
            self.out(f"[Error] {e.code().name}: {e.details()}")
        except ValueError as e:
            self.out(f"[Client] {e}")
        return True

    def run(self):
        self.out(f"Connected to server as '{self.name}'.", MENU)
        while True:
            line = self.prompt("> ")
            if not self.dispatch(line):
                break
        self.close()
        self.out("[Client] Bye.")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(2)
    address = sys.argv[1]
    client_name = sys.argv[2] if len(sys.argv) > 2 else f"{os.uname().nodename}:{os.getpid()}"
    Client(address, client_name).run()
