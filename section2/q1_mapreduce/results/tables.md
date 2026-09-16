# Weather analytics — MapReduce vs MPI results

## Inputs

| size | records | bytes | sequential T1 (s) | HDFS put (s) |
|---|---|---|---|---|

## MapReduce, two-stage design: job time (s), median over repetitions

## Design variants (8 maps)

| size | two-stage r=4 | one-stage (mapper → 1 reducer) | two-stage, no combiner r=4 | map output records (with / without combiner) | shuffle bytes (with / without) |
|---|---|---|---|---|---|

## Head-to-head: best configuration of each

| size | sequential | MPI best (P) | MR best (maps, reducers) | MR / MPI | MR throughput MB/s | MPI throughput MB/s |
|---|---|---|---|---|---|---|
