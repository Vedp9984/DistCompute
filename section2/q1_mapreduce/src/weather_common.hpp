// Shared machinery for the Q8 weather-analytics programs.
//
// The sequential and MPI implementations share the record parser, the
// accumulator, the tie-break comparators and the report writer, so the only
// thing that differs between them is *how the accumulator gets filled and
// combined* -- which is exactly the part the assignment is about.
#ifndef WEATHER_COMMON_HPP
#define WEATHER_COMMON_HPP

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <string>
#include <unordered_map>
#include <vector>

namespace wx {

// A measurement counts as "extreme" at or beyond these bounds.
constexpr double kHotThreshold = 40.0;
constexpr double kColdThreshold = 0.0;
// The assignment fixes the bucket width for the busiest-interval statistic.
constexpr long long kIntervalSeconds = 60;

// ---------------------------------------------------------------------------
// Neumaier compensated summation.
//
// Worth the few extra flops here: the global sums run over N terms (N is in the
// millions for the benchmark sizes) and the *point* of the correctness check is
// that the MPI output matches the sequential output.  Plain double summation
// over 10^7 terms accumulates enough rounding drift that the two orderings --
// one long chain sequentially, P shorter chains combined under MPI -- can
// disagree in the second decimal place.  Compensating locally makes each chain
// near-exact, so the only remaining difference is the P-term combination.
// ---------------------------------------------------------------------------
class KSum {
public:
    void add(double x) {
        const double t = sum_ + x;
        if (std::fabs(sum_) >= std::fabs(x)) comp_ += (sum_ - t) + x;
        else                                 comp_ += (x - t) + sum_;
        sum_ = t;
    }
    double value() const { return sum_ + comp_; }

private:
    double sum_ = 0.0;
    double comp_ = 0.0;
};

// ---------------------------------------------------------------------------
// A single extreme measurement, with the assignment's tie-break rule:
// higher (resp. lower) temperature wins; ties go to the smaller timestamp, then
// the smaller station id.  MPI_MAXLOC cannot express a two-level tie-break,
// which is why the MPI program reduces these by hand.
// ---------------------------------------------------------------------------
struct Extremum {
    double temp = 0.0;
    long long station = 0;
    long long ts = 0;
    bool valid = false;
};

inline bool hotter(const Extremum& a, const Extremum& b) {  // is a a better "hottest" than b?
    if (!b.valid) return a.valid;
    if (!a.valid) return false;
    if (a.temp != b.temp) return a.temp > b.temp;
    if (a.ts != b.ts) return a.ts < b.ts;
    return a.station < b.station;
}

inline bool colder(const Extremum& a, const Extremum& b) {
    if (!b.valid) return a.valid;
    if (!a.valid) return false;
    if (a.temp != b.temp) return a.temp < b.temp;
    if (a.ts != b.ts) return a.ts < b.ts;
    return a.station < b.station;
}

// ---------------------------------------------------------------------------
// Accumulator -- everything one process learns from the records it parsed.
// ---------------------------------------------------------------------------
struct Accum {
    long long count = 0;
    long long extreme = 0;

    KSum sum_t, sum_h, sum_p, sum_rain, sum_wind;

    double min_t = std::numeric_limits<double>::infinity();
    double max_t = -std::numeric_limits<double>::infinity();
    double min_h = std::numeric_limits<double>::infinity();
    double max_h = -std::numeric_limits<double>::infinity();
    double min_p = std::numeric_limits<double>::infinity();
    double max_p = -std::numeric_limits<double>::infinity();
    double max_rain = -std::numeric_limits<double>::infinity();
    double max_wind = -std::numeric_limits<double>::infinity();

    Extremum hottest, coldest;

    // Per-station, indexed by station id.  Grown on demand so a station id
    // beyond the declared S does not corrupt anything.
    //
    // These are compensated too, not just the global sums.  It looks like
    // overkill for a few hundred terms, but a station average landing exactly
    // on the 2-decimal rounding boundary is not rare -- `tests/tiny.txt` has
    // one, where the true sum is 112.25 over 10 readings, i.e. 11.225 exactly.
    // There, plain summation gives 112.25000000000001 in one chain and 112.25
    // in two, which prints as 11.23 and 11.22.  Compensating makes both chains
    // land on the correctly-rounded value.
    std::vector<long long> st_count;
    std::vector<KSum> st_temp_sum;
    std::vector<KSum> st_rain_sum;

    // Sparse: only the 60-second buckets that actually occur.
    std::unordered_map<long long, long long> intervals;

    void reserve_stations(long long S) {
        if (S <= 0) return;
        st_count.assign(static_cast<size_t>(S), 0);
        st_temp_sum.assign(static_cast<size_t>(S), KSum{});
        st_rain_sum.assign(static_cast<size_t>(S), KSum{});
    }

    void ensure_station(long long id) {
        if (id < 0) return;
        if (static_cast<size_t>(id) < st_count.size()) return;
        const size_t want = static_cast<size_t>(id) + 1;
        st_count.resize(want);
        st_temp_sum.resize(want);
        st_rain_sum.resize(want);
    }

    void add(long long ts, long long station, double temp, double hum,
             double pres, double rain, double wind) {
        ++count;

        sum_t.add(temp);   sum_h.add(hum);    sum_p.add(pres);
        sum_rain.add(rain); sum_wind.add(wind);

        if (temp < min_t) min_t = temp;
        if (temp > max_t) max_t = temp;
        if (hum  < min_h) min_h = hum;
        if (hum  > max_h) max_h = hum;
        if (pres < min_p) min_p = pres;
        if (pres > max_p) max_p = pres;
        if (rain > max_rain) max_rain = rain;
        if (wind > max_wind) max_wind = wind;

        if (temp >= kHotThreshold || temp <= kColdThreshold) ++extreme;

        const Extremum here{temp, station, ts, true};
        if (hotter(here, hottest)) hottest = here;
        if (colder(here, coldest)) coldest = here;

        ensure_station(station);
        if (station >= 0) {
            st_count[static_cast<size_t>(station)] += 1;
            st_temp_sum[static_cast<size_t>(station)].add(temp);
            st_rain_sum[static_cast<size_t>(station)].add(rain);
        }

        // Integer division floors toward zero; timestamps are non-negative in
        // this dataset, so `ts / 60` is the bucket the assignment asks for.
        intervals[ts / kIntervalSeconds] += 1;
    }
};

// ---------------------------------------------------------------------------
// Record parsing
// ---------------------------------------------------------------------------
// One line: timestamp station_id temperature humidity pressure rainfall wind
// Returns false for a line that does not hold seven fields (blank lines, a
// trailing partial line), which the caller simply skips.
inline bool parse_line(char* begin, char* end, long long& ts, long long& station,
                       double& temp, double& hum, double& pres, double& rain,
                       double& wind) {
    const char saved = *end;
    *end = '\0';                       // bound strtod/strtoll to this line
    char* p = begin;
    char* q = nullptr;

    ts = std::strtoll(p, &q, 10);          if (q == p) { *end = saved; return false; }
    p = q; station = std::strtoll(p, &q, 10); if (q == p) { *end = saved; return false; }
    p = q; temp = std::strtod(p, &q);       if (q == p) { *end = saved; return false; }
    p = q; hum  = std::strtod(p, &q);       if (q == p) { *end = saved; return false; }
    p = q; pres = std::strtod(p, &q);       if (q == p) { *end = saved; return false; }
    p = q; rain = std::strtod(p, &q);       if (q == p) { *end = saved; return false; }
    p = q; wind = std::strtod(p, &q);       if (q == p) { *end = saved; return false; }

    *end = saved;
    return true;
}

// Fold every complete line in [begin, end) into `acc`.  The buffer is mutated
// in place (a NUL is parked at each line end and restored), so it must be
// writable.
inline void parse_buffer(char* begin, char* end, Accum& acc) {
    char* p = begin;
    while (p < end) {
        char* nl = static_cast<char*>(std::memchr(p, '\n', static_cast<size_t>(end - p)));
        char* line_end = nl ? nl : end;
        if (line_end > p) {
            long long ts, station;
            double temp, hum, pres, rain, wind;
            if (parse_line(p, line_end, ts, station, temp, hum, pres, rain, wind))
                acc.add(ts, station, temp, hum, pres, rain, wind);
        }
        if (!nl) break;
        p = nl + 1;
    }
}

// ---------------------------------------------------------------------------
// Header:  N K S
// ---------------------------------------------------------------------------
inline bool parse_header(const char* buf, size_t len, long long& N, long long& K,
                         long long& S, size_t& header_bytes) {
    const char* nl = static_cast<const char*>(std::memchr(buf, '\n', len));
    const size_t hlen = nl ? static_cast<size_t>(nl - buf) : len;
    std::string line(buf, hlen);
    if (std::sscanf(line.c_str(), "%lld %lld %lld", &N, &K, &S) != 3) return false;
    header_bytes = nl ? hlen + 1 : hlen;
    return true;
}

// ---------------------------------------------------------------------------
// Final report
// ---------------------------------------------------------------------------
struct StationRow {
    long long id = 0;
    long long count = 0;
    double avg_temp = 0.0;
    double total_rain = 0.0;
};

struct Report {
    long long count = 0;
    double avg_t = 0, min_t = 0, max_t = 0;
    double avg_h = 0, min_h = 0, max_h = 0;
    double avg_p = 0, min_p = 0, max_p = 0;
    double total_rain = 0, max_rain = 0;
    double avg_wind = 0, max_wind = 0;
    long long extreme = 0;
    Extremum hottest, coldest;
    long long busy_interval = 0, busy_count = 0;
    std::vector<StationRow> top;
};

// Rank stations by decreasing count, then increasing id, and keep the first K
// that were actually observed.  Stations with no measurements are omitted --
// a station that reported nothing is not among the "top stations by measurement
// count" in any useful sense.
inline std::vector<StationRow> top_stations(const std::vector<long long>& cnt,
                                            const std::vector<double>& tsum,
                                            const std::vector<double>& rsum,
                                            long long K) {
    std::vector<StationRow> rows;
    rows.reserve(cnt.size());
    for (size_t i = 0; i < cnt.size(); ++i) {
        if (cnt[i] <= 0) continue;
        rows.push_back({static_cast<long long>(i), cnt[i],
                        tsum[i] / static_cast<double>(cnt[i]), rsum[i]});
    }
    const size_t keep = std::min<size_t>(rows.size(),
                                         K > 0 ? static_cast<size_t>(K) : 0);
    std::partial_sort(rows.begin(), rows.begin() + static_cast<long>(keep),
                      rows.end(), [](const StationRow& a, const StationRow& b) {
                          if (a.count != b.count) return a.count > b.count;
                          return a.id < b.id;
                      });
    rows.resize(keep);
    return rows;
}

// KSum is two doubles, so an array of them is not something MPI_Reduce can sum
// directly.  Flatten to the compensated values just before the reduction.
inline std::vector<double> materialize(const std::vector<KSum>& v) {
    std::vector<double> out(v.size());
    for (size_t i = 0; i < v.size(); ++i) out[i] = v[i].value();
    return out;
}

// Busiest 60-second bucket: highest count wins, ties go to the smaller
// interval id.  An empty map yields (0, 0).
inline void pick_busiest(const std::unordered_map<long long, long long>& counts,
                         long long& best_id, long long& best_count) {
    best_id = 0;
    best_count = 0;
    bool have = false;
    for (const auto& kv : counts) {
        if (!have || kv.second > best_count ||
            (kv.second == best_count && kv.first < best_id)) {
            best_id = kv.first;
            best_count = kv.second;
            have = true;
        }
    }
}

// All floating-point output is fixed at two decimals.  A single documented
// precision is what makes `diff` a usable correctness check between the
// sequential and MPI runs.
inline void write_report(std::FILE* out, const Report& r) {
    std::fprintf(out, "TOTAL_MEASUREMENTS %lld\n", r.count);
    std::fprintf(out, "AVERAGE_TEMPERATURE %.2f\n", r.avg_t);
    std::fprintf(out, "MIN_TEMPERATURE %.2f\n", r.min_t);
    std::fprintf(out, "MAX_TEMPERATURE %.2f\n", r.max_t);
    std::fprintf(out, "AVERAGE_HUMIDITY %.2f\n", r.avg_h);
    std::fprintf(out, "MIN_HUMIDITY %.2f\n", r.min_h);
    std::fprintf(out, "MAX_HUMIDITY %.2f\n", r.max_h);
    std::fprintf(out, "AVERAGE_PRESSURE %.2f\n", r.avg_p);
    std::fprintf(out, "MIN_PRESSURE %.2f\n", r.min_p);
    std::fprintf(out, "MAX_PRESSURE %.2f\n", r.max_p);
    std::fprintf(out, "TOTAL_RAINFALL %.2f\n", r.total_rain);
    std::fprintf(out, "MAX_RAINFALL %.2f\n", r.max_rain);
    std::fprintf(out, "AVERAGE_WIND_SPEED %.2f\n", r.avg_wind);
    std::fprintf(out, "MAX_WIND_SPEED %.2f\n", r.max_wind);
    std::fprintf(out, "EXTREME_TEMPERATURE_EVENTS %lld\n", r.extreme);
    std::fprintf(out, "HOTTEST_MEASUREMENT %.2f %lld %lld\n",
                 r.hottest.temp, r.hottest.station, r.hottest.ts);
    std::fprintf(out, "COLDEST_MEASUREMENT %.2f %lld %lld\n",
                 r.coldest.temp, r.coldest.station, r.coldest.ts);
    std::fprintf(out, "BUSIEST_INTERVAL %lld %lld\n",
                 r.busy_interval, r.busy_count);
    std::fprintf(out, "TOP_STATIONS\n");
    for (const StationRow& s : r.top) {
        std::fprintf(out, "%lld %lld %.2f %.2f\n",
                     s.id, s.count, s.avg_temp, s.total_rain);
    }
}

// Turn aggregated totals into the report.  Shared so the sequential and MPI
// programs cannot drift apart in how they divide or how they treat N = 0.
inline void finalize(Report& r, long long count, double sum_t, double sum_h,
                     double sum_p, double sum_rain, double sum_wind,
                     double min_t, double max_t, double min_h, double max_h,
                     double min_p, double max_p, double max_rain,
                     double max_wind, long long extreme) {
    r.count = count;
    r.extreme = extreme;
    if (count > 0) {
        const double n = static_cast<double>(count);
        r.avg_t = sum_t / n;
        r.avg_h = sum_h / n;
        r.avg_p = sum_p / n;
        r.avg_wind = sum_wind / n;
        r.min_t = min_t; r.max_t = max_t;
        r.min_h = min_h; r.max_h = max_h;
        r.min_p = min_p; r.max_p = max_p;
        r.max_rain = max_rain;
        r.max_wind = max_wind;
    }
    // With no records every field stays 0.00 rather than +/-inf.
    r.total_rain = sum_rain;
}

}  // namespace wx

#endif  // WEATHER_COMMON_HPP
