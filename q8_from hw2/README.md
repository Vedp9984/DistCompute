# Q8 — Large-Scale Weather and Environmental Data Analytics

Sequential and MPI implementations of the required statistics over a stream of
station measurements, plus a reproducible dataset generator.

## Files

| Path | Purpose |
|---|---|
| `src/weather_mpi.cpp` | MPI implementation (per-rank parallel read + reductions) |
| `src/weather_seq.cpp` | Sequential implementation — oracle and `T₁` baseline |
| `src/weather_common.hpp` | Parser, accumulator, tie-breaks, report writer |
| `src/gen_weather.cpp` | Reproducible dataset generator |
| `scripts/oracle.py` | Independent Python reference, written from the spec |
| `scripts/gen_data.sh` | Generates the four benchmark datasets |
| `scripts/verify.sh` | Three-layer correctness suite |
| `scripts/benchmark.sh` | Timing sweep → `results/q8_timings.csv` + plots |
| `tests/` | Small inputs with verified expected output |
| `slurm/q8_job.sh` | Cluster job: build → generate → verify → benchmark |

## Build

```bash
make                                  # bin/weather_mpi, weather_seq, gen_weather
module load hpcx-2.7.0/hpcx-ompi      # on the cluster, before make
```

## Run

```bash
bin/weather_seq data/weather_small.txt                    # sequential
mpirun -np 8 bin/weather_mpi data/weather_small.txt       # MPI
mpirun -np 8 bin/weather_mpi input.txt --output report.txt --time
```

Add `--oversubscribe` when the machine has fewer cores than `P`.

| Flag | Meaning |
|---|---|
| `--output FILE` | Write the report to `FILE` instead of stdout |
| `--time` | Print a timing breakdown **on stderr** |

**stdout carries only the report.**

## Input format

```
N K S
timestamp station_id temperature humidity pressure rainfall wind_speed    ×N
```

`N` measurements, top-`K` requested, `S` stations with ids `0 … S−1`.

## Output format

Exactly the block given in the assignment: the scalar statistics, then
`HOTTEST_MEASUREMENT`, `COLDEST_MEASUREMENT`, `BUSIEST_INTERVAL`, then
`TOP_STATIONS` followed by one line per station.

### Documented output conventions

The assignment fixes the field order but not the formatting, so these are the
choices made here — a single documented precision is what makes `diff` a usable
correctness check between the sequential and MPI runs:

- **All floating-point values print with exactly 2 decimals** (`%.2f`).
- **Counts and ids print as plain integers.**
- **Interval id is `timestamp / 60`** (integer division), as specified.
- **Ties**: top stations by decreasing count, then increasing station id;
  hottest/coldest by temperature, then smaller timestamp, then smaller station
  id; busiest interval by count, then smaller interval id.
- **Stations with zero measurements are omitted** from `TOP_STATIONS` — a
  station that reported nothing is not "top by measurement count". If fewer
  than `K` stations reported, fewer than `K` lines are printed.
- **Empty input (`N = 0`)**: every numeric field prints `0.00` / `0`, the
  hottest/coldest lines print `0.00 0 0`, and `TOP_STATIONS` has no rows —
  rather than `inf`, which is what an unguarded min/max would produce.

## How the work is distributed

### Parallel input — the part that actually matters

The obvious design is "rank 0 reads the file and scatters the records". That
does not scale: the serial read is `O(file)` on one rank at every `P`, so by
Amdahl it caps the speedup no matter how fast the analysis gets. On a 3 GB input
it *is* the runtime.

Instead every rank reads its **own byte range**, using plain POSIX I/O:

```
rank 0 reads the "N K S" header and broadcasts it (4 longs)
the remaining bytes are cut into P equal ranges
each rank streams its range in 8 MB chunks, carrying the partial record
    that straddles a chunk boundary into the next chunk
a rank whose range starts mid-record discards up to its first '\n'
    (decided by looking at the byte before its start, so a range that begins
     exactly on a record boundary keeps that record instead of dropping it)
each rank parses through the end of the record that *starts* before its end,
    reading past its own end if necessary to finish that record
```

A record belongs to the rank whose range contains its **first byte**. The rank
before it holds the tail of that record inside its overlap window and parses it
to completion, and the rank after it skips past it. So every record is processed
exactly once with **no communication at all** for the split — which is the
property `scripts/verify.sh` hammers with a prime record count, so that no
boundary can land tidily.

### Reductions

| Statistic | How it combines |
|---|---|
| counts, sums | `MPI_Reduce` `MPI_SUM` — 2 longs + 5 doubles |
| min/max temp, humidity, pressure | `MPI_Reduce` `MPI_MIN` / `MPI_MAX` — 3 + 5 doubles |
| hottest / coldest | `MPI_Gather` of one candidate per rank, comparator on rank 0 |
| per-station table | `MPI_Reduce` `MPI_SUM` over dense arrays of length `S` |
| busiest interval | dense `MPI_Reduce` over the global bucket range, sparse `MPI_Gatherv` fallback |

Everything except the last two is **constant size** — independent of `N`. That
is why the communication term stays flat while the computation term falls as
`N/P`.

**Why hottest/coldest are not `MPI_MAXLOC`.** `MPI_MAXLOC` pairs a value with a
single index and breaks ties on that index alone. The assignment wants ties
broken on *timestamp first, then station id* — two levels. So each rank
contributes one local candidate and rank 0 applies the real comparator over the
`P` of them. `P` is tiny; this costs nothing and is exactly right.

**Why the interval histogram has two paths.** Bucket ids are `timestamp / 60`
and can be spread arbitrarily far apart. When the global span is ≤ 8M buckets
(≈15 years of wall-clock) a dense `MPI_Reduce` over that range is simplest and
fastest. Beyond that the dense array would cost more memory than the data, so
the ranks ship only the buckets they actually saw via `MPI_Gatherv` and rank 0
merges them. Real inputs take the dense path.

## Implementation notes

- **Compensated (Neumaier) summation everywhere — including the per-station
  sums.** This is the subtlest thing in the program and it is worth being
  precise about why.

  Floating-point addition is not associative, so summing `N` values in one chain
  (sequential) and in `P` chains combined (MPI) can differ in the last bit. That
  is usually invisible. It stops being invisible when a printed value lands
  exactly on a rounding boundary. `tests/tiny.txt` has such a case: station 0's
  ten readings total exactly `112.25`, so its average is `11.225` — precisely on
  the 2-decimal boundary.

  ```
  one chain  : 112.25000000000001  →  11.225000000000001  →  prints 11.23
  two chains : 112.25              →  11.224999999999999  →  prints 11.22
  ```

  Both are "correct" summations; they differ by one ULP, and the printed digit
  flips. Compensating makes each chain produce the *correctly rounded* sum
  (`112.25`), so sequential and MPI land on the same value — and it is the more
  accurate value, which is the real justification. `scripts/oracle.py` uses
  Python's `math.fsum`, which is exactly rounded, and the C++ result agrees with
  it: that agreement is the evidence the compensation actually works.

  The per-station sums are compensated too, even though they run over far fewer
  terms — as the example above shows, it is the *boundary*, not the number of
  terms, that decides whether the error is visible.

  Honest caveat: compensation makes disagreement vanishingly unlikely, not
  impossible. Bit-exact agreement between different reduction orderings is not
  something IEEE-754 guarantees. The suite compares with `diff` and currently
  passes exactly; a future input that lands on a boundary *after* compensation
  would need a tolerance-based comparison rather than a code change.
- **Station ids beyond the declared `S`** do not corrupt anything: the
  per-station arrays grow on demand, and an `MPI_Allreduce(MAX)` on the array
  length agrees on a common size before the reduction.
- **In-place line parsing.** `parse_line` parks a `NUL` at the line end so
  `strtod`/`strtoll` cannot run past it into the next record, then restores the
  byte. No per-line allocation or copy.
- **Why POSIX I/O and not MPI-IO.** `MPI_File_read_at` is the textbook choice
  here and it was the first implementation. It works on a local disk and
  **deadlocks on a cluster**: MPI-IO's ROMIO layer needs file locking on a
  network filesystem, and on an NFS-mounted home directory that locking is
  commonly unavailable, so concurrent readers block indefinitely. This was not
  theoretical — three separate cluster jobs froze at P = 4, one of them for three
  hours until the scheduler killed it, while the same binary passed all 65 checks
  on a laptop.

  Nothing is lost by using `fopen`/`fseek`/`fread` instead. The parallelism comes
  from each rank reading a *disjoint byte range*, which POSIX reads serve just as
  well; MPI-IO earns its keep for shared-file **writes** and collective access
  patterns, and this program has neither. `fread` additionally reports how many
  bytes it actually delivered, so a short read on a busy filesystem truncates the
  chunk rather than leaving stale buffer contents to be parsed.

- **Sequential streaming.** `weather_seq` reads 8 MB blocks with a carry-over
  for the record straddling a block boundary, so it never needs the file in
  memory either — the two implementations are compared on equal footing.

## Dataset generation

```bash
bash scripts/gen_data.sh              # the four benchmark datasets
SEED=99 bash scripts/gen_data.sh      # a different reproducible draw

bin/gen_weather N S K [--seed SEED] [--span SECONDS] [--output FILE]
```

### Reproducibility

Every value is a pure function of `(N, S, K, seed, span)`. The generator uses a
hand-written **SplitMix64** with Box–Muller normals rather than `<random>`,
because the C++ standard pins down the *engines* but not the *distributions* —
`std::normal_distribution` legitimately produces different values under
different standard libraries. A dataset that differed between the laptop and the
cluster would make the two sets of benchmark numbers incomparable. With this
generator, the same command produces byte-identical files anywhere, which is why
`sync.sh` regenerates data on the cluster instead of uploading it.

### The model

| Field | Distribution |
|---|---|
| `timestamp` | `1700000000 + U{0, span−1}`, unsorted |
| `station_id` | `⌊S·u²⌋`, `u ~ U(0,1)` — **skewed**, low ids more frequent |
| `temperature` | `base(station) + 7.5·sin(2π(tod − 0.25)) + N(0, 3.5)` |
| `humidity` | `base(station) − 0.8·diurnal + N(0, 6)`, clipped to `[0, 100]` |
| `pressure` | `base(station) + N(0, 4)`, clipped to `[870, 1085]` |
| `rainfall` | `0` with prob. `1 − wet(station)`, else `−3·ln u` (exponential) |
| `wind_speed` | `|N(0, 7)|`, clipped to `[0, 60]` |

Per-station baselines are drawn once: `base_temp ~ U(4, 34) °C`,
`base_hum ~ U(30, 85) %`, `base_pres ~ U(985, 1025) hPa`, `wet ~ U(0.02, 0.30)`.

**Assumptions, and why they were made:**

- *Station choice is skewed, not uniform.* Under a uniform draw every station
  gets ≈ `N/S` measurements and the top-K ranking is decided by sampling noise —
  useless for checking the tie-break rule. `u²` concentrates mass on low ids, so
  the ranking is stable and reproducible.
- *Temperature carries a diurnal cycle.* A flat distribution would make
  `AVERAGE_TEMPERATURE` and the per-station averages nearly identical and would
  make the hottest/coldest results uninteresting. The amplitude and noise width
  are set so a small but non-zero fraction of readings crosses the ≥ 40 °C /
  ≤ 0 °C extreme thresholds, exercising `EXTREME_TEMPERATURE_EVENTS`.
- *Rainfall is zero-inflated.* Real precipitation is zero most of the time;
  a uniform draw would make `TOTAL_RAINFALL` a boring multiple of `N`.
- *Timestamps are unsorted.* None of the required statistics depends on order,
  and unsorted input is the harder case for the interval histogram.

### Benchmark datasets

| Label | N | S | span | ≈ size |
|---|---|---|---|---|
| small | 1 000 000 | 500 | 1 day | ~55 MB |
| medium | 5 000 000 | 2 000 | 3 days | ~275 MB |
| large | 20 000 000 | 5 000 | 7 days | ~1.1 GB |
| xlarge | 50 000 000 | 10 000 | 14 days | ~2.8 GB |

`span` widens with `N` so the interval histogram keeps a comparable number of
buckets; otherwise "busiest interval" would degenerate to "every interval".

## Correctness verification

```bash
make check          # or: bash scripts/verify.sh
```

Three layers, because any two alone would leave a gap:

1. **Sequential vs. stored expected output** for every file in `tests/` —
   catches regressions.
2. **Sequential vs. `scripts/oracle.py`**, an independent Python implementation
   written from the assignment text. `weather_seq` and `weather_mpi` share
   `weather_common.hpp`, so they can share a *misreading of the spec*; the
   oracle cannot.
3. **MPI at P = 1, 2, 4, 8 vs. sequential**, on the fixed cases and on generated
   datasets: `N = 200003` (prime, so no byte-range boundary lands tidily), 3
   stations vs. 4000 stations, a 60-second span that crams everything into one
   bucket, and `N = 7` and `N = 1` where most ranks get no records at all.

`tests/handmade.txt` is hand-constructed so every discrete answer is known in
advance: two records tie at 40.00 °C and two at −5.00 °C with equal timestamps
(so the tie-break must pick station 0 both times), two stations tie at 3
measurements (so the top-K must order 0 before 1), and 4 records land in the
same 60-second bucket.

## Benchmarking

```bash
make bench          # generates data if needed, then benchmarks
sbatch slurm/q8_job.sh
```

Four dataset sizes × `P ∈ {1,2,4,8}` × 3 repetitions, plus the sequential
program on the same inputs. Writes `results/q8_timings.csv`,
`results/q8_sequential.csv`, the figures, and `results/q8_tables.md`.

## Cost model — what the plots should show

Per rank: `N/P` records read and parsed. Communication: a fixed handful of
scalars, plus `S` values for the station table and `R` for the interval
histogram — **none of which depend on `N`**.

```
computation / communication ≈ (N/P) / (S + R + c)
```

So this problem is far more favourable than Q1 or Q5: the parallel part grows
with `N` while the serial part does not. Expect near-linear speedup on the large
datasets, with two limits visible in the plots:

- **I/O bandwidth.** Parsing is fast; on a shared filesystem the reads can
  saturate before the CPUs do, so the `read=` term in the `--time` output is
  the one to watch when speedup falls short.
- **Small inputs.** On the `small` dataset the fixed cost of the collectives and
  MPI start-up is a visible fraction of a sub-second run, so its curve flattens
  earliest — the usual strong-scaling floor.
