#!/bin/bash
# Push the repo to the cluster (~/HW3).  Binaries are rebuilt there, datasets
# regenerated (seeded).  results/, demo/, data/, logs/ are produced ON the
# cluster and pulled back with pull.sh -- they are excluded here so --delete
# never touches them.
rsync -az --delete \
  --exclude 'bin/' --exclude 'data/' --exclude 'logs/' --exclude 'results/' --exclude 'demo/' \
  --exclude '*.o' --exclude '__pycache__/' --exclude 'hadoop/conf-*' --exclude '.verify.*' \
  "$(dirname "$0")/" cs3401.20@rce.iiit.ac.in:~/HW3/
