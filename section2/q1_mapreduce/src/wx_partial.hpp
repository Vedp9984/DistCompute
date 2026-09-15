// The intermediate representation shared by the weather MapReduce programs.
//
// Three kinds of key/value line flow from mapper to reducer:
//
//   G     \t <global partial>          one per mapper: counts, compensated sums,
//                                       min/max, hottest/coldest candidate
//   S:<id> \t count temp_sum rain_sum   one per station a mapper saw
//   I:<b>  \t count                     one per 60-second bucket a mapper saw
//   H     \t N K S                      the input header, forwarded so the final
//                                       stage knows K without help from the driver
//
// The reducer merges lines with equal keys and emits the *same* format, so the
// one program serves as combiner, as stage-1 reducer, and (with a single
// reducer) as a complete one-stage solution.
//
// All doubles travel as C99 hex floats ("%a").  Decimal formatting would either
// lose bits or need 17 digits and a careful parser; hex floats round-trip
// exactly, so the Neumaier-compensated sums survive the shuffle intact and the
// report matches the sequential program's to the last printed digit.
#ifndef WX_PARTIAL_HPP
#define WX_PARTIAL_HPP

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

#include "weather_common.hpp"

namespace wxmr {

struct Global {
    long long count = 0, extreme = 0;
    wx::KSum sum_t, sum_h, sum_p, sum_rain, sum_wind;
    double min_t = INFINITY, max_t = -INFINITY;
    double min_h = INFINITY, max_h = -INFINITY;
    double min_p = INFINITY, max_p = -INFINITY;
    double max_rain = -INFINITY, max_wind = -INFINITY;
    wx::Extremum hottest, coldest;

    void merge_from(const Global& o) {
        count += o.count; extreme += o.extreme;
        sum_t.add(o.sum_t.value()); sum_h.add(o.sum_h.value()); sum_p.add(o.sum_p.value());
        sum_rain.add(o.sum_rain.value()); sum_wind.add(o.sum_wind.value());
        min_t = std::min(min_t, o.min_t); max_t = std::max(max_t, o.max_t);
        min_h = std::min(min_h, o.min_h); max_h = std::max(max_h, o.max_h);
        min_p = std::min(min_p, o.min_p); max_p = std::max(max_p, o.max_p);
        max_rain = std::max(max_rain, o.max_rain); max_wind = std::max(max_wind, o.max_wind);
        if (wx::hotter(o.hottest, hottest)) hottest = o.hottest;
        if (wx::colder(o.coldest, coldest)) coldest = o.coldest;
    }

    // Lift the per-process accumulator into a Global.
    static Global from_accum(const wx::Accum& a) {
        Global g;
        g.count = a.count; g.extreme = a.extreme;
        g.sum_t = a.sum_t; g.sum_h = a.sum_h; g.sum_p = a.sum_p;
        g.sum_rain = a.sum_rain; g.sum_wind = a.sum_wind;
        g.min_t = a.min_t; g.max_t = a.max_t; g.min_h = a.min_h; g.max_h = a.max_h;
        g.min_p = a.min_p; g.max_p = a.max_p; g.max_rain = a.max_rain; g.max_wind = a.max_wind;
        g.hottest = a.hottest; g.coldest = a.coldest;
        return g;
    }

    void write(std::FILE* out) const {
        std::fprintf(out, "G\t%lld %lld %a %a %a %a %a %a %a %a %a %a %a %a %a %d %a %lld %lld %d %a %lld %lld\n",
                     count, extreme,
                     sum_t.value(), sum_h.value(), sum_p.value(), sum_rain.value(), sum_wind.value(),
                     min_t, max_t, min_h, max_h, min_p, max_p, max_rain, max_wind,
                     hottest.valid ? 1 : 0, hottest.temp, hottest.station, hottest.ts,
                     coldest.valid ? 1 : 0, coldest.temp, coldest.station, coldest.ts);
    }

    // Parse the value part (after the tab).  Returns false on a malformed line.
    bool parse(const char* v) {
        char* p = const_cast<char*>(v);
        auto ll = [&]() { return std::strtoll(p, &p, 10); };
        auto d  = [&]() { return std::strtod(p, &p); };
        count = ll(); extreme = ll();
        double st = d(), sh = d(), sp = d(), sr = d(), sw = d();
        sum_t = wx::KSum{}; sum_t.add(st); sum_h = wx::KSum{}; sum_h.add(sh);
        sum_p = wx::KSum{}; sum_p.add(sp); sum_rain = wx::KSum{}; sum_rain.add(sr);
        sum_wind = wx::KSum{}; sum_wind.add(sw);
        min_t = d(); max_t = d(); min_h = d(); max_h = d(); min_p = d(); max_p = d();
        max_rain = d(); max_wind = d();
        hottest.valid = ll() != 0; hottest.temp = d(); hottest.station = ll(); hottest.ts = ll();
        coldest.valid = ll() != 0; coldest.temp = d(); coldest.station = ll(); coldest.ts = ll();
        return true;
    }
};

struct StationPartial {
    long long count = 0;
    wx::KSum temp_sum, rain_sum;
};

inline void write_station(std::FILE* out, long long id, const StationPartial& s) {
    std::fprintf(out, "S:%lld\t%lld %a %a\n", id, s.count, s.temp_sum.value(), s.rain_sum.value());
}

inline void write_interval(std::FILE* out, long long bucket, long long count) {
    std::fprintf(out, "I:%lld\t%lld\n", bucket, count);
}

inline bool read_line(std::string& out) {
    static char buf[1 << 16];
    out.clear();
    while (std::fgets(buf, sizeof buf, stdin)) {
        out += buf;
        if (out.back() == '\n') break;
    }
    if (out.empty()) return false;
    while (!out.empty() && (out.back() == '\n' || out.back() == '\r')) out.pop_back();
    return true;
}

}  // namespace wxmr

#endif
