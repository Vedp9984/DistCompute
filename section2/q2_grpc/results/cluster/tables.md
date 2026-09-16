# gRPC streaming — results

All runs: 44/44 final reports byte-identical to `weather_seq`.

## A. Number of workers (batch 1000, round-robin, pre-parsed stream)

| dataset | workers | throughput rec/s | processing time s | coordinator CPU s | Σ worker CPU s | worker RSS MB |
|---|---|---|---|---|---|---|
| weather_small.txt | 1 | 528,288 | 1.89 | 0.5 | 2.0 | 47 |
| weather_small.txt | 2 | 1,057,692 | 0.95 | 0.5 | 2.0 | 44 |
| weather_small.txt | 4 | 1,063,750 | 0.94 | 0.6 | 3.6 | 43 |
| weather_small.txt | 8 | 1,073,136 | 0.93 | 0.6 | 3.7 | 40 |
| weather_small.txt | 12 | 1,068,069 | 0.94 | 0.6 | 3.8 | 38 |
| weather_medium.txt | 1 | 491,058 | 10.18 | 2.3 | 10.8 | 50 |
| weather_medium.txt | 2 | 1,008,750 | 4.96 | 2.0 | 10.5 | 50 |
| weather_medium.txt | 4 | 1,081,854 | 4.62 | 2.1 | 18.1 | 58 |
| weather_medium.txt | 8 | 1,064,480 | 4.70 | 2.2 | 18.4 | 51 |
| weather_medium.txt | 12 | 1,051,764 | 4.75 | 2.4 | 18.8 | 48 |

### A2. Same, but the streamer parses the text file while sending

| workers | throughput rec/s |
|---|---|
| 1 | 506,202 |
| 2 | 753,165 |
| 4 | 749,927 |
| 8 | 749,226 |

## B. Message granularity (4 workers, round-robin, small dataset)

| records / message | messages | throughput rec/s | processing time s | coordinator CPU s |
|---|---|---|---|---|
| 1 | 1,000,000 | 10,207 | 97.97 | 156.7 |
| 10 | 100,000 | 101,048 | 9.90 | 15.9 |
| 100 | 10,000 | 858,650 | 1.16 | 2.0 |
| 1000 | 1,000 | 1,082,711 | 0.92 | 0.5 |
| 10000 | 100 | 1,113,767 | 0.90 | 0.5 |
| 100000 | 10 | 1,036,113 | 0.97 | 0.5 |

## C. Distribution strategy (batch 1000, medium dataset)

| workers | round-robin rec/s | station-hash rec/s | coordinator CPU s (rr / station) |
|---|---|---|---|
| 2 | 979,008 | 931,841 | 2.2 / 6.2 |
| 4 | 1,090,829 | 910,971 | 2.1 / 7.3 |
| 8 | 1,059,455 | 764,464 | 2.4 / 8.9 |

## D. Concurrent query clients during ingest (4 workers, batch 1000, medium)

| dist | query clients | ingest rec/s | queries served | QPS | latency p50 ms | p95 ms | p99 ms |
|---|---|---|---|---|---|---|---|
| rr | 1 | 812,032 | 112 | 18 | 52.8 | 66.8 | 121.9 |
| rr | 4 | 661,356 | 223 | 30 | 134.0 | 180.4 | 227.7 |
| rr | 8 | 657,961 | 214 | 28 | 282.0 | 367.4 | 399.4 |
| rr | 16 | 662,112 | 208 | 28 | 583.8 | 822.6 | 948.4 |
| station | 1 | 504,677 | 275 | 28 | 35.5 | 45.7 | 54.5 |
| station | 4 | 224,072 | 1059 | 47 | 84.0 | 101.6 | 106.8 |
| station | 8 | 213,000 | 1103 | 47 | 170.8 | 190.3 | 197.6 |
| station | 16 | 210,294 | 1119 | 47 | 339.0 | 393.7 | 410.0 |

## E. Throttled source (200 000 rec/s target, 4 query clients)

achieved 198,038 rec/s; queries: QPS 108, p50 36.6 ms, p99 51.8 ms

