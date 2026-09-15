#!/bin/bash
# Scripted local run of the PDF 1.5 demo (server + 2 clients on this machine).
# Produces demo/local_*.txt transcripts.  For the 3-node cluster version see demo_cluster.sbatch.
set -u
cd "$(dirname "$0")"; mkdir -p demo
PY=${PY:-$HOME/hw3venv/bin/python}; ADDR=${ADDR:-localhost:50051}
$PY server.py 0.0.0.0:50051 > demo/local_server.log 2>&1 & SRV=$!
sleep 1.5
# Client 1: create + open, then wait for client 2 to subscribe, then edit twice.
(echo 'create report.txt "Hello World"'; sleep 1; echo "open report.txt"; sleep 6
 echo 'edit report.txt 6 "Distributed "'; sleep 2; echo 'edit report.txt 0 "[v2] "'; sleep 2; echo "exit") \
  | $PY client.py $ADDR client1 > demo/local_client1.txt 2>&1 &
C1=$!
sleep 3
# Client 2: open, subscribe, sit and receive the two updates, re-open, exit.
(echo "open report.txt"; sleep 1; echo "subscribe report.txt"; sleep 9; echo "open report.txt"; sleep 1; echo "exit") \
  | $PY client.py $ADDR client2 > demo/local_client2.txt 2>&1 &
C2=$!
wait $C1 $C2
$PY concurrent_demo.py $ADDR --clients 2 --doc concurrent2.txt > demo/local_concurrent_2clients.txt 2>&1
$PY concurrent_demo.py $ADDR --clients 8 --doc concurrent8.txt > demo/local_concurrent_8clients.txt 2>&1
$PY test_functional.py $ADDR > demo/local_functional_test.txt 2>&1
kill $SRV; wait $SRV 2>/dev/null
for f in demo/local_client1.txt demo/local_client2.txt demo/local_concurrent_2clients.txt demo/local_server.log; do echo "=================== $f"; cat "$f"; done
