# Section 3, Problem 1 — Collaborative Document Editing with gRPC

A central document server keeps documents in memory; any number of clients
create, read and edit them, and clients subscribed to a document are pushed
its new content over a server-streaming RPC every time it changes.

| File | Purpose |
|---|---|
| `document.proto` | `DocumentService` and its messages |
| `server.py` | in-memory documents, per-document locking, subscriber fan-out |
| `client.py` | interactive CLI (menu 1–5 or typed commands), background update listener |
| `concurrent_demo.py` | N clients editing the same position at the same instant, with checks |
| `test_functional.py` | every RPC and every error path, against a running server |
| `demo_local.sh` | scripted version of the PDF §1.5 demo on one machine |
| `demo_cluster.sbatch` | the same demo on three RCE nodes (server / client 1 / client 2) |
| `demo/` | captured transcripts from both |

## Setup

```bash
python3 -m venv ~/hw3venv && ~/hw3venv/bin/pip install grpcio grpcio-tools
# regenerate the stubs after editing the proto:
~/hw3venv/bin/python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. document.proto
```

On RCE: `module load python/3.12.5` first (the system `python3` is 3.6).

## Running

```bash
python3 server.py 0.0.0.0:50051                 # server; the address is a CLI argument
python3 client.py node01:50051 [client-name]    # any number of clients
```

Bind the server to `0.0.0.0`, not `localhost`, when clients are on other
nodes (RCE guide §7). The optional client name is what other subscribers see
in `edited_by`.

Client commands (menu number or word):

```
1 / create <name> "<initial content>"
2 / open <name>
3 / edit <name> <position> "<text>"
4 / subscribe <name>
5 / exit
    unsubscribe <name>
```

## Design

### Interface (`document.proto`)

The four required RPCs, plus a monotonically increasing per-document
`version` in every response and update. The version is what lets a client
tell whether an `[Update]` it just received is newer than what it last saw —
important because updates arrive on a background thread while the user may
be issuing `open` on the foreground thread. `DocumentUpdate` also carries
`edited_by`, `position`, `inserted_text`, so a subscriber can show *what*
changed, not only the new text.

`SubscribeToUpdates` is a server-streaming RPC: one request, an unbounded
stream of `DocumentUpdate` messages until the client cancels. The **first
message is a snapshot** (`snapshot = true`, current content and version) sent
under the document lock at subscription time. It gives the subscriber a
consistent starting point and a positive confirmation that the subscription is
live — without it a client cannot tell whether it is subscribed or the stream is
merely quiet, which is exactly the race `concurrent_demo.py` first tripped
over on the cluster (editors released before the server had registered the
observer).

### Server concurrency

Two levels of lock, always taken in the same order (registry → document), so
there is no deadlock:

* `registry_lock` guards the `name → Document` dict, held only for create and
  lookup — never while a document lock is held.
* Each `Document` has its own `lock`, held for the read–modify–write of an
  edit **and** for enqueuing the resulting update to every subscriber. Two
  clients inserting at position 6 at the same instant are serialised: the
  second one runs after the first has committed and inserts into the already
  modified string. Neither edit is lost or torn, and the version numbers they
  get back tell them the order. (The PDF explicitly does not require OT/CRDT;
  "apply in the order processed" is what this does.)

Edits to *different* documents run fully in parallel: gRPC's thread pool
(64 threads) services them and only the per-document lock is contended.

### Update propagation

Each subscriber owns a `queue.Queue`. An edit, under the document lock, `put`s
the update on every queue — an O(#subscribers) memory operation that never
blocks on the network, so a slow or dead subscriber cannot stall an editing
client. Each `SubscribeToUpdates` handler drains its own queue and `yield`s
messages into the gRPC stream. The handler wakes once a second to check
`context.is_active()` and also registers `context.add_callback`, so a
subscriber that disconnects (Ctrl-C, network drop) is removed from the list
promptly.

### Client

A subscription starts a daemon thread iterating the stream and printing
`[Update] ...` lines; the main thread keeps reading commands. A print lock
keeps a background update from interleaving with a foreground response
mid-line. `exit` cancels all open streams so the server sees a clean
unsubscribe.

### Errors → gRPC status codes

| Situation | Code |
|---|---|
| create with a name already in use | `ALREADY_EXISTS` |
| create with an empty name | `INVALID_ARGUMENT` |
| open / edit / subscribe a document that does not exist | `NOT_FOUND` |
| insert position past the end of the document | `OUT_OF_RANGE` |

The client prints them as `[Error] CODE: details`.

## Correctness

`test_functional.py` (run against a live server) covers create/get, every
error code above, insert at 0 / middle / end, an empty insert (still a new
version), that a subscriber receives exactly the edits made while subscribed,
in version order, and nothing after unsubscribing:

```
$ python3 test_functional.py localhost:50051
== create / get ==
  [ok] create message
  [ok] get returns initial content, version 0
== error paths ==
  [ok] ALREADY_EXISTS: document 'test_….txt' already exists
  [ok] INVALID_ARGUMENT: document name must not be empty
  [ok] NOT_FOUND: document 'nope.txt' does not exist
  [ok] NOT_FOUND: document 'nope.txt' does not exist
  [ok] OUT_OF_RANGE: position 99 is beyond the end of the document (length 11)
  [ok] NOT_FOUND: document 'nope.txt' does not exist
== subscribe + edit ==
  [ok] subscription starts with a snapshot
  [ok] insert at 6 -> 'Hello Distributed World'
  [ok] insert at 0 (prepend)
  [ok] insert at len (append)
  [ok] empty insert still bumps version
  [ok] subscriber got 4 updates
  [ok] in order
  [ok] last update content matches
  [ok] no updates after unsubscribe
RESULT: PASS
```

`concurrent_demo.py` releases N editors from a barrier so their
`EditDocument` calls hit the server simultaneously, while an observer is
subscribed, then checks length, version count, that every insert is present
and contiguous, that the versions handed out are a permutation of 1..N, and
that the observer saw N updates in order ending with the final document.

## Demonstration

Full transcripts are in `demo/`. The cluster run (`demo_cluster.sbatch`) uses
three nodes exactly as the RCE guide describes — server on the first,
one client on each of the other two — and the transcripts below are from that
run; `demo_local.sh` reproduces the same thing on one machine.

**Step 1–2. Client 1 creates and opens; Client 2 opens** (client 1, node 2):

```
> create report.txt "Hello World"
[Client] Document report.txt created.
> open report.txt
[Client] Hello World
```

**Step 3. Client 2 subscribes** (client 2, node 3):

```
> open report.txt
[Client] Hello World
> subscribe report.txt
[Client] Subscribed to updates (current version 0).
```

**Step 4. Client 1 edits; Client 2 receives the update without asking:**

client 1:
```
> edit report.txt 6 "Distributed "
[Client] Edit applied (version 1).
[Client] Hello Distributed World
```

client 2 (nothing typed — the line arrives on the subscription stream):
```
[Update] Document report.txt modified (v1, by client1: inserted 'Distributed ' at 6).
Hello Distributed World
```

**Step 5. Two clients edit at position 6 at the same time** (`concurrent_demo.py`):

```
[demo] created concurrent2.txt: 'Hello World'
[demo] 2 clients inserting at position 6 simultaneously ...
[Update] v1 by client2   -> 'Hello Systems World'
[Update] v2 by client1   -> 'Hello Distributed Systems World'
[client1] inserted 'Distributed ' -> got version 2 (1.9 ms): 'Hello Distributed Systems World'
[client2] inserted 'Systems ' -> got version 1 (1.8 ms): 'Hello Systems World'
[demo] final document (v2): 'Hello Distributed Systems World'
  [ok] length 31 == 31
  [ok] version 2 == 2
  [ok] every insert present and untorn
  [ok] each edit got a distinct consecutive version
  [ok] subscriber received 2 updates
  [ok] updates arrived in version order
  [ok] last update equals final document
[demo] RESULT: PASS
```

Which of the two lands first is decided by whichever request the server's
thread pool dequeues first; both are applied, the document is never
corrupted, and the 8-client variant (`demo/*concurrent_8clients.txt`) passes
the same checks.

Server log for the run (who edited what, and how many subscribers were
notified):

```
[server] created report.txt           (11 chars) by ipv4:…
[server] subscribe report.txt         by client2 (now 1)
[server] edit    report.txt           v1    pos=6    +12  chars by client1 -> 1 subscriber(s)
[server] edit    report.txt           v2    pos=0    +5   chars by client1 -> 1 subscriber(s)
[server] unsubscribe report.txt       by client2 (now 0)
```
