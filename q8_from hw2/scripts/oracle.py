#!/usr/bin/env python3
"""Independent Python reference for Q8, written straight from the assignment
text rather than from the C++ code.

Its job is to catch a shared misreading of the spec: `weather_seq` and
`weather_mpi` agreeing with each other proves the parallelisation is sound, but
not that either one implements what was asked.  This does.

    python3 scripts/oracle.py INPUT
"""

import math
import sys
from collections import defaultdict


def main(path):
    with open(path) as fh:
        n, k, s = (int(x) for x in fh.readline().split())

        # Values are collected and summed with math.fsum, which is exactly
        # rounded.  Naive left-to-right addition is off by an ULP often enough
        # to matter: in tests/tiny.txt one station's readings total exactly
        # 112.25 over 10 samples, i.e. an average of 11.225 -- precisely on the
        # 2-decimal rounding boundary, where a 1-ULP error flips the printed
        # digit.  The C++ implementations use Neumaier compensation for the same
        # reason; agreeing with fsum here is what shows that compensation works.
        count = 0
        t_vals, h_vals, p_vals, rain_vals, wind_vals = [], [], [], [], []
        extreme = 0
        hottest = coldest = None
        st = defaultdict(lambda: [0, [], []])         # count, temps, rainfalls
        intervals = defaultdict(int)

        for line in fh:
            parts = line.split()
            if len(parts) < 7:
                continue
            ts = int(parts[0])
            sid = int(parts[1])
            t, h, p, r, w = (float(x) for x in parts[2:7])

            count += 1
            t_vals.append(t); h_vals.append(h); p_vals.append(p)
            rain_vals.append(r); wind_vals.append(w)
            if t >= 40.0 or t <= 0.0:
                extreme += 1

            # Ties: smaller timestamp, then smaller station id.
            cand = (t, ts, sid)
            if hottest is None or (-cand[0], cand[1], cand[2]) < \
                                  (-hottest[0], hottest[1], hottest[2]):
                hottest = cand
            if coldest is None or (cand[0], cand[1], cand[2]) < \
                                  (coldest[0], coldest[1], coldest[2]):
                coldest = cand

            e = st[sid]
            e[0] += 1; e[1].append(t); e[2].append(r)
            intervals[ts // 60] += 1

    sum_t = math.fsum(t_vals)
    sum_h = math.fsum(h_vals)
    sum_p = math.fsum(p_vals)
    sum_rain = math.fsum(rain_vals)
    sum_wind = math.fsum(wind_vals)
    min_t = min(t_vals, default=0.0); max_t = max(t_vals, default=0.0)
    min_h = min(h_vals, default=0.0); max_h = max(h_vals, default=0.0)
    min_p = min(p_vals, default=0.0); max_p = max(p_vals, default=0.0)
    max_rain = max(rain_vals, default=0.0); max_wind = max(wind_vals, default=0.0)

    out = []
    out.append(f"TOTAL_MEASUREMENTS {count}")
    div = count if count else 1
    zero = count == 0
    out.append(f"AVERAGE_TEMPERATURE {0.0 if zero else sum_t / div:.2f}")
    out.append(f"MIN_TEMPERATURE {0.0 if zero else min_t:.2f}")
    out.append(f"MAX_TEMPERATURE {0.0 if zero else max_t:.2f}")
    out.append(f"AVERAGE_HUMIDITY {0.0 if zero else sum_h / div:.2f}")
    out.append(f"MIN_HUMIDITY {0.0 if zero else min_h:.2f}")
    out.append(f"MAX_HUMIDITY {0.0 if zero else max_h:.2f}")
    out.append(f"AVERAGE_PRESSURE {0.0 if zero else sum_p / div:.2f}")
    out.append(f"MIN_PRESSURE {0.0 if zero else min_p:.2f}")
    out.append(f"MAX_PRESSURE {0.0 if zero else max_p:.2f}")
    out.append(f"TOTAL_RAINFALL {sum_rain:.2f}")
    out.append(f"MAX_RAINFALL {0.0 if zero else max_rain:.2f}")
    out.append(f"AVERAGE_WIND_SPEED {0.0 if zero else sum_wind / div:.2f}")
    out.append(f"MAX_WIND_SPEED {0.0 if zero else max_wind:.2f}")
    out.append(f"EXTREME_TEMPERATURE_EVENTS {extreme}")
    ht = hottest or (0.0, 0, 0)
    cd = coldest or (0.0, 0, 0)
    out.append(f"HOTTEST_MEASUREMENT {ht[0]:.2f} {ht[2]} {ht[1]}")
    out.append(f"COLDEST_MEASUREMENT {cd[0]:.2f} {cd[2]} {cd[1]}")

    if intervals:
        bid, bcount = min(((-c, i) for i, c in intervals.items()))[1], \
                      max(intervals.values())
        bid = min(i for i, c in intervals.items() if c == bcount)
    else:
        bid, bcount = 0, 0
    out.append(f"BUSIEST_INTERVAL {bid} {bcount}")

    out.append("TOP_STATIONS")
    rows = sorted(((sid, v[0], math.fsum(v[1]) / v[0], math.fsum(v[2]))
                   for sid, v in st.items() if v[0] > 0),
                  key=lambda r: (-r[1], r[0]))
    for sid, c, avg, rain in rows[:k]:
        out.append(f"{sid} {c} {avg:.2f} {rain:.2f}")

    print("\n".join(out))
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(__doc__, file=sys.stderr)
        sys.exit(2)
    sys.exit(main(sys.argv[1]))
