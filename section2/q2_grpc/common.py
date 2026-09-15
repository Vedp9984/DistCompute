"""Analytics state shared by worker, coordinator and dashboard.

A Python port of HW2's ``wx::Accum`` with the same semantics:

* Neumaier-compensated sums (so W workers' partial sums merged together land
  on the same 2-decimal value the sequential program prints);
* hottest/coldest tie-break: temperature, then smaller timestamp, then smaller
  station id;
* top-K by decreasing count then increasing station id, stations with no
  measurements omitted;
* busiest interval = ``timestamp // 60`` with the most records, ties to the
  smaller bucket id;
* N = 0 prints zeros, never inf.

``Partial`` is what a worker holds and what travels in ``PartialState``;
``merge`` folds partials together; ``report_lines`` renders the exact HW2 text.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field

import weather_pb2 as pb

HOT, COLD, INTERVAL = 40.0, 0.0, 60
INF = float("inf")


class KSum:
    """Neumaier compensated summation."""
    __slots__ = ("s", "c")

    def __init__(self, s: float = 0.0, c: float = 0.0):
        self.s, self.c = s, c

    def add(self, x: float) -> None:
        t = self.s + x
        if abs(self.s) >= abs(x):
            self.c += (self.s - t) + x
        else:
            self.c += (x - t) + self.s
        self.s = t

    def value(self) -> float:
        return self.s + self.c


@dataclass
class Extremum:
    valid: bool = False
    temp: float = 0.0
    station: int = 0
    ts: int = 0

    def to_pb(self) -> pb.Extremum:
        return pb.Extremum(valid=self.valid, temp=self.temp, station=self.station, timestamp=self.ts)

    @staticmethod
    def from_pb(e: pb.Extremum) -> "Extremum":
        return Extremum(e.valid, e.temp, e.station, e.timestamp)


def hotter(a: Extremum, b: Extremum) -> bool:
    if not b.valid:
        return a.valid
    if not a.valid:
        return False
    if a.temp != b.temp:
        return a.temp > b.temp
    if a.ts != b.ts:
        return a.ts < b.ts
    return a.station < b.station


def colder(a: Extremum, b: Extremum) -> bool:
    if not b.valid:
        return a.valid
    if not a.valid:
        return False
    if a.temp != b.temp:
        return a.temp < b.temp
    if a.ts != b.ts:
        return a.ts < b.ts
    return a.station < b.station


@dataclass
class Partial:
    count: int = 0
    extreme: int = 0
    sum_t: KSum = field(default_factory=KSum)
    sum_h: KSum = field(default_factory=KSum)
    sum_p: KSum = field(default_factory=KSum)
    sum_rain: KSum = field(default_factory=KSum)
    sum_wind: KSum = field(default_factory=KSum)
    min_t: float = INF; max_t: float = -INF
    min_h: float = INF; max_h: float = -INF
    min_p: float = INF; max_p: float = -INF
    max_rain: float = -INF
    max_wind: float = -INF
    hottest: Extremum = field(default_factory=Extremum)
    coldest: Extremum = field(default_factory=Extremum)
    # station -> [count, KSum temp, KSum rain]
    stations: dict = field(default_factory=dict)
    intervals: dict = field(default_factory=dict)
    batches_applied: int = 0

    # -- ingest ---------------------------------------------------------------
    def add_batch(self, b: pb.RecordBatch) -> int:
        """Fold one columnar batch in.  Returns the number of records."""
        n = len(b.timestamp)
        if n == 0:
            return 0
        st_tab, iv_tab = self.stations, self.intervals
        sum_t, sum_h, sum_p, sum_rain, sum_wind = self.sum_t, self.sum_h, self.sum_p, self.sum_rain, self.sum_wind
        min_t, max_t = self.min_t, self.max_t
        min_h, max_h = self.min_h, self.max_h
        min_p, max_p = self.min_p, self.max_p
        max_rain, max_wind = self.max_rain, self.max_wind
        extreme = 0
        hottest, coldest = self.hottest, self.coldest
        for ts, sid, temp, hum, pres, rain, wind in zip(
                b.timestamp, b.station_id, b.temperature, b.humidity, b.pressure, b.rainfall, b.wind_speed):
            sum_t.add(temp); sum_h.add(hum); sum_p.add(pres); sum_rain.add(rain); sum_wind.add(wind)
            if temp < min_t: min_t = temp
            if temp > max_t: max_t = temp
            if hum < min_h: min_h = hum
            if hum > max_h: max_h = hum
            if pres < min_p: min_p = pres
            if pres > max_p: max_p = pres
            if rain > max_rain: max_rain = rain
            if wind > max_wind: max_wind = wind
            if temp >= HOT or temp <= COLD:
                extreme += 1
            if temp >= hottest.temp or not hottest.valid:
                here = Extremum(True, temp, sid, ts)
                if hotter(here, hottest):
                    hottest = here
            if temp <= coldest.temp or not coldest.valid:
                here = Extremum(True, temp, sid, ts)
                if colder(here, coldest):
                    coldest = here
            s = st_tab.get(sid)
            if s is None:
                s = st_tab[sid] = [0, KSum(), KSum()]
            s[0] += 1
            s[1].add(temp)
            s[2].add(rain)
            bucket = ts // INTERVAL
            iv_tab[bucket] = iv_tab.get(bucket, 0) + 1
        self.count += n
        self.extreme += extreme
        self.min_t, self.max_t = min_t, max_t
        self.min_h, self.max_h = min_h, max_h
        self.min_p, self.max_p = min_p, max_p
        self.max_rain, self.max_wind = max_rain, max_wind
        self.hottest, self.coldest = hottest, coldest
        self.batches_applied += 1
        return n

    # -- merge --------------------------------------------------------------------
    def merge(self, o: "Partial") -> None:
        self.count += o.count
        self.extreme += o.extreme
        self.sum_t.add(o.sum_t.value()); self.sum_h.add(o.sum_h.value()); self.sum_p.add(o.sum_p.value())
        self.sum_rain.add(o.sum_rain.value()); self.sum_wind.add(o.sum_wind.value())
        self.min_t = min(self.min_t, o.min_t); self.max_t = max(self.max_t, o.max_t)
        self.min_h = min(self.min_h, o.min_h); self.max_h = max(self.max_h, o.max_h)
        self.min_p = min(self.min_p, o.min_p); self.max_p = max(self.max_p, o.max_p)
        self.max_rain = max(self.max_rain, o.max_rain); self.max_wind = max(self.max_wind, o.max_wind)
        if hotter(o.hottest, self.hottest):
            self.hottest = o.hottest
        if colder(o.coldest, self.coldest):
            self.coldest = o.coldest
        for sid, (c, t, r) in o.stations.items():
            s = self.stations.get(sid)
            if s is None:
                self.stations[sid] = [c, KSum(t.s, t.c), KSum(r.s, r.c)]
            else:
                s[0] += c; s[1].add(t.value()); s[2].add(r.value())
        for k, v in o.intervals.items():
            self.intervals[k] = self.intervals.get(k, 0) + v
        self.batches_applied += o.batches_applied

    # -- wire ---------------------------------------------------------------------
    def to_pb(self, worker_id: str = "") -> pb.PartialState:
        m = pb.PartialState(
            count=self.count, extreme=self.extreme,
            sum_t=self.sum_t.value(), sum_h=self.sum_h.value(), sum_p=self.sum_p.value(),
            sum_rain=self.sum_rain.value(), sum_wind=self.sum_wind.value(),
            min_t=self.min_t, max_t=self.max_t, min_h=self.min_h, max_h=self.max_h,
            min_p=self.min_p, max_p=self.max_p, max_rain=self.max_rain, max_wind=self.max_wind,
            hottest=self.hottest.to_pb(), coldest=self.coldest.to_pb(),
            batches_applied=self.batches_applied, worker_id=worker_id)
        m.stations.extend(pb.StationPartial(station=s, count=c, temp_sum=t.value(), rain_sum=r.value())
                          for s, (c, t, r) in self.stations.items())
        m.intervals.extend(pb.IntervalPartial(bucket=k, count=v) for k, v in self.intervals.items())
        return m

    @staticmethod
    def from_pb(m: pb.PartialState) -> "Partial":
        p = Partial(count=m.count, extreme=m.extreme,
                    sum_t=KSum(m.sum_t), sum_h=KSum(m.sum_h), sum_p=KSum(m.sum_p),
                    sum_rain=KSum(m.sum_rain), sum_wind=KSum(m.sum_wind),
                    min_t=m.min_t, max_t=m.max_t, min_h=m.min_h, max_h=m.max_h,
                    min_p=m.min_p, max_p=m.max_p, max_rain=m.max_rain, max_wind=m.max_wind,
                    hottest=Extremum.from_pb(m.hottest), coldest=Extremum.from_pb(m.coldest),
                    batches_applied=m.batches_applied)
        # An empty worker reports 0.0 for its (unset) min/max; treat as "nothing seen".
        if p.count == 0:
            p.min_t = p.min_h = p.min_p = INF
            p.max_t = p.max_h = p.max_p = p.max_rain = p.max_wind = -INF
        p.stations = {s.station: [s.count, KSum(s.temp_sum), KSum(s.rain_sum)] for s in m.stations}
        p.intervals = {i.bucket: i.count for i in m.intervals}
        return p


def merge_all(partials) -> Partial:
    total = Partial()
    for p in partials:
        total.merge(p)
    return total


# -- report ------------------------------------------------------------------------
def top_stations(p: Partial, k: int):
    rows = [(sid, c, t.value() / c, r.value()) for sid, (c, t, r) in p.stations.items() if c > 0]
    rows.sort(key=lambda x: (-x[1], x[0]))
    return rows[: max(k, 0)]


def busiest(p: Partial):
    best_id, best_c = 0, 0
    for k, v in p.intervals.items():
        if v > best_c or (v == best_c and best_c > 0 and k < best_id):
            best_id, best_c = k, v
    return best_id, best_c


def fill_report(rep: pb.AnalyticsReport, p: Partial, k: int) -> None:
    n = p.count
    rep.total_measurements = n
    rep.extreme_temperature_events = p.extreme
    rep.total_rainfall = p.sum_rain.value()
    if n > 0:
        rep.avg_temperature = p.sum_t.value() / n
        rep.avg_humidity = p.sum_h.value() / n
        rep.avg_pressure = p.sum_p.value() / n
        rep.avg_wind_speed = p.sum_wind.value() / n
        rep.min_temperature, rep.max_temperature = p.min_t, p.max_t
        rep.min_humidity, rep.max_humidity = p.min_h, p.max_h
        rep.min_pressure, rep.max_pressure = p.min_p, p.max_p
        rep.max_rainfall, rep.max_wind_speed = p.max_rain, p.max_wind
    rep.hottest.CopyFrom(p.hottest.to_pb())
    rep.coldest.CopyFrom(p.coldest.to_pb())
    rep.busiest_interval, rep.busiest_count = busiest(p)
    del rep.top_stations[:]
    rep.top_stations.extend(pb.StationRow(station=s, count=c, avg_temp=a, total_rain=r)
                            for s, c, a, r in top_stations(p, k))
    rep.stations_seen = len(p.stations)
    rep.intervals_seen = len(p.intervals)


def report_lines(r: pb.AnalyticsReport) -> list[str]:
    """Exactly the HW2 output block, for `diff` against weather_seq."""
    out = [
        f"TOTAL_MEASUREMENTS {r.total_measurements}",
        f"AVERAGE_TEMPERATURE {r.avg_temperature:.2f}",
        f"MIN_TEMPERATURE {r.min_temperature:.2f}",
        f"MAX_TEMPERATURE {r.max_temperature:.2f}",
        f"AVERAGE_HUMIDITY {r.avg_humidity:.2f}",
        f"MIN_HUMIDITY {r.min_humidity:.2f}",
        f"MAX_HUMIDITY {r.max_humidity:.2f}",
        f"AVERAGE_PRESSURE {r.avg_pressure:.2f}",
        f"MIN_PRESSURE {r.min_pressure:.2f}",
        f"MAX_PRESSURE {r.max_pressure:.2f}",
        f"TOTAL_RAINFALL {r.total_rainfall:.2f}",
        f"MAX_RAINFALL {r.max_rainfall:.2f}",
        f"AVERAGE_WIND_SPEED {r.avg_wind_speed:.2f}",
        f"MAX_WIND_SPEED {r.max_wind_speed:.2f}",
        f"EXTREME_TEMPERATURE_EVENTS {r.extreme_temperature_events}",
        f"HOTTEST_MEASUREMENT {r.hottest.temp:.2f} {r.hottest.station} {r.hottest.timestamp}",
        f"COLDEST_MEASUREMENT {r.coldest.temp:.2f} {r.coldest.station} {r.coldest.timestamp}",
        f"BUSIEST_INTERVAL {r.busiest_interval} {r.busiest_count}",
        "TOP_STATIONS",
    ]
    out += [f"{s.station} {s.count} {s.avg_temp:.2f} {s.total_rain:.2f}" for s in r.top_stations]
    return out


# -- dataset reading ----------------------------------------------------------------
def read_header(fh):
    n, k, s = (int(x) for x in fh.readline().split())
    return pb.Header(n=n, k=k, s=s)


def iter_batches(fh, batch_size: int, limit: int = 0):
    """Yield columnar RecordBatch messages from an open dataset file (header already consumed)."""
    seq = 0
    sent = 0
    while True:
        b = pb.RecordBatch(seq=seq)
        ts, sid, te, hu, pr, ra, wi = [], [], [], [], [], [], []
        for _ in range(batch_size):
            line = fh.readline()
            if not line:
                break
            f = line.split()
            if len(f) != 7:
                continue
            ts.append(int(f[0])); sid.append(int(f[1])); te.append(float(f[2])); hu.append(float(f[3]))
            pr.append(float(f[4])); ra.append(float(f[5])); wi.append(float(f[6]))
            sent += 1
            if limit and sent >= limit:
                break
        if not ts:
            return
        b.timestamp.extend(ts); b.station_id.extend(sid); b.temperature.extend(te); b.humidity.extend(hu)
        b.pressure.extend(pr); b.rainfall.extend(ra); b.wind_speed.extend(wi)
        yield b
        seq += 1
        if limit and sent >= limit:
            return
