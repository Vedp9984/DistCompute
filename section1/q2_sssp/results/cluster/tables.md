# SSSP on Hadoop — results

## Runs

| graph | V | E | mode | reducers | maps | iterations | total s | s / iteration |
|---|---|---|---|---|---|---|---|---|
| chain40 | 40 | 39 | frontier | 2 | 2 | 40 | 720.8 | 17.2 |
| rnd1k | 1000 | 5000 | frontier | 2 | 2 | 14 | 286.1 | 17.9 |
| star10k | 10000 | 10000 | frontier | 2 | 2 | 2 | 74.3 | 18.6 |
| rnd10k_unreach | 10000 | 30000 | frontier | 2 | 2 | 22 | 420.7 | 17.5 |
| layered10k | 10000 | 40000 | frontier | 2 | 2 | 100 | 1784.1 | 17.5 |
| rnd10k | 10000 | 50000 | frontier | 2 | 2 | 22 | 426.8 | 17.8 |
| rnd10k | 10000 | 50000 | full | 2 | 2 | 22 | 427.0 | 17.8 |
| rnd10k_dense | 10000 | 500000 | frontier | 1 | 4 | 19 | 372.9 | 17.8 |
| rnd10k_dense | 10000 | 500000 | frontier | 2 | 2 | 19 | 372.2 | 17.7 |
| rnd10k_dense | 10000 | 500000 | frontier | 4 | 4 | 19 | 362.3 | 17.3 |
| rnd10k_dense | 10000 | 500000 | frontier | 4 | 8 | 19 | 365.7 | 17.4 |
| rnd10k_dense | 10000 | 500000 | frontier | 8 | 4 | 19 | 399.4 | 19.0 |
| rnd10k_dense | 10000 | 500000 | full | 4 | 4 | 19 | 367.8 | 17.5 |
