# Weather analytics — MapReduce vs MPI results

## Inputs

| size | records | bytes | sequential T1 (s, best of 2) | HDFS put (s) |
|---|---|---|---|---|
| small | 1,000,000 | 43,977,074 | 0.48 | 1.6 |
| medium | 5,000,000 | 222,880,466 | 2.40 | 1.8 |
| large | 20,000,000 | 899,000,733 | 9.61 | 2.3 |
| xlarge | 50,000,000 | 2,256,997,981 | 24.57 | 3.1 |

## MapReduce, two-stage design: job time (s), median over repetitions

**small** (1,000,000 records, sequential 0.48 s)

| maps \ reducers | r=1 | r=4 | stage-1 s | final s | Σ map-task time s |
|---|---|---|---|---|---|
| m=2 | 32.7 | 34.2 | 15.4 | 14.4 | 4.8 |
| m=4 | 34.2 | 34.3 | 16.4 | 14.9 | 11.8 |
| m=8 | 36.3 | 37.8 | 18.4 | 14.9 | 38.7 |
| m=16 | 41.4 | 42.4 | 23.1 | 15.4 | 146.4 |
| m=32 | 49.6 | 51.6 | 31.3 | 15.4 | 584.7 |

**medium** (5,000,000 records, sequential 2.40 s)

| maps \ reducers | r=1 | r=4 | stage-1 s | final s | Σ map-task time s |
|---|---|---|---|---|---|
| m=2 | 35.3 | 35.2 | 16.9 | 15.4 | 7.1 |
| m=4 | 35.7 | 35.7 | 17.4 | 15.4 | 14.8 |
| m=8 | 37.9 | 38.3 | 19.5 | 15.4 | 43.5 |
| m=16 | 42.3 | 42.7 | 24.7 | 14.6 | 160.0 |
| m=32 | 49.6 | 50.6 | 31.7 | 14.9 | 615.0 |

**large** (20,000,000 records, sequential 9.61 s)

| maps \ reducers | r=1 | r=4 | stage-1 s | final s | Σ map-task time s |
|---|---|---|---|---|---|
| m=2 | 42.0 | 41.9 | 23.6 | 15.4 | 19.0 |
| m=4 | 39.3 | 42.0 | 19.9 | 16.4 | 23.5 |
| m=8 | 45.7 | 45.4 | 24.6 | 17.5 | 79.5 |
| m=16 | 43.6 | 45.2 | 25.7 | 14.9 | 193.2 |
| m=32 | 63.8 | 67.4 | 43.7 | 16.5 | 948.3 |

**xlarge** (50,000,000 records, sequential 24.57 s)

| maps \ reducers | r=1 | r=4 | stage-1 s | final s | Σ map-task time s |
|---|---|---|---|---|---|
| m=2 | 52.9 | 52.4 | 34.5 | 15.4 | 42.4 |
| m=4 | 43.8 | 43.8 | 25.5 | 15.4 | 44.4 |
| m=8 | 44.4 | 43.8 | 25.0 | 16.4 | 88.1 |
| m=16 | 46.5 | 48.0 | 28.7 | 14.9 | 243.2 |
| m=32 | 57.7 | 59.1 | 39.8 | 14.9 | 810.7 |

## Design variants (8 maps)

| size | two-stage r=4 (s) | one-stage: mapper → 1 reducer (s) | two-stage without combiner, r=4 (s) | map output records | shuffle bytes |
|---|---|---|---|---|---|
| small | 37.8 | 21.4 | 35.8 | 15,537 | 404,400 |
| medium | 38.3 | 22.4 | 37.8 | 50,577 | 1,464,937 |
| large | 45.4 | 25.2 | 38.9 | 120,657 | 3,576,720 |
| xlarge | 43.8 | 32.7 | 48.7 | 241,297 | 7,162,391 |

## MPI (HW2 program, same nodes): wall time (s), median of repetitions

| size | P=1 | P=2 | P=4 | P=8 | P=16 |
|---|---|---|---|---|---|
| small | 0.72 | 0.52 | 0.39 | 0.32 | 0.29 |
| medium | 2.65 | 1.58 | 0.94 | 0.61 | 0.43 |
| large | 9.91 | 5.47 | 2.89 | 1.61 | 1.01 |
| xlarge | 24.86 | 13.43 | 6.89 | 3.65 | 2.10 |

## Head-to-head: best configuration of each

| size | sequential | MPI best (P) | MR best (maps, reducers) | MR / MPI | MR throughput MB/s | MPI throughput MB/s |
|---|---|---|---|---|---|---|
| small | 0.48 | 0.29 (P=16) | 32.7 (m=2, r=1) | 113× | 1 | 145 |
| medium | 2.40 | 0.43 (P=16) | 35.2 (m=2, r=4) | 82× | 6 | 494 |
| large | 9.61 | 1.01 (P=16) | 39.3 (m=4, r=1) | 39× | 22 | 849 |
| xlarge | 24.57 | 2.10 (P=16) | 43.8 (m=4, r=4) | 21× | 49 | 1025 |
