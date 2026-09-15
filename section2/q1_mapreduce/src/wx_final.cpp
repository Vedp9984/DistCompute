// Weather analytics, final stage (one reducer).
//
// Consumes the merged partials -- however many stages produced them -- and
// writes the report in the exact HW2 format.  Reuses wx::finalize,
// wx::top_stations, wx::pick_busiest and wx::write_report from the shared
// header so the MapReduce report cannot drift from the sequential one in how
// it divides, orders, or handles N = 0.
//
// K comes from the forwarded H line.  WX_K in the environment overrides it
// (handy for running the final stage on partials whose header was lost).
#include <unordered_map>

#include "wx_partial.hpp"

int main() {
    wxmr::Global g;
    std::unordered_map<long long, wxmr::StationPartial> stations;
    std::unordered_map<long long, long long> intervals;
    long long K = -1;
    if (const char* e = std::getenv("WX_K")) K = std::atoll(e);

    std::string line;
    while (wxmr::read_line(line)) {
        size_t tab = line.find('\t');
        if (tab == std::string::npos) continue;
        const char* v = line.c_str() + tab + 1;
        switch (line[0]) {
            case 'G': { wxmr::Global o; o.parse(v); g.merge_from(o); break; }
            case 'S': {
                long long id = std::atoll(line.c_str() + 2);
                char* p = const_cast<char*>(v);
                auto& s = stations[id];
                s.count += std::strtoll(p, &p, 10);
                s.temp_sum.add(std::strtod(p, &p));
                s.rain_sum.add(std::strtod(p, &p));
                break;
            }
            case 'I': intervals[std::atoll(line.c_str() + 2)] += std::atoll(v); break;
            case 'H': {
                long long n, k, s;
                if (K < 0 && std::sscanf(v, "%lld %lld %lld", &n, &k, &s) == 3) K = k;
                break;
            }
        }
    }
    if (K < 0) K = 0;

    wx::Report rep;
    wx::finalize(rep, g.count, g.sum_t.value(), g.sum_h.value(), g.sum_p.value(),
                 g.sum_rain.value(), g.sum_wind.value(), g.min_t, g.max_t, g.min_h,
                 g.max_h, g.min_p, g.max_p, g.max_rain, g.max_wind, g.extreme);
    rep.hottest = g.hottest;
    rep.coldest = g.coldest;

    // Dense arrays indexed by station id, as top_stations expects.
    long long maxid = -1;
    for (const auto& kv : stations) maxid = std::max(maxid, kv.first);
    std::vector<long long> cnt(maxid + 1, 0);
    std::vector<double> tsum(maxid + 1, 0.0), rsum(maxid + 1, 0.0);
    for (const auto& kv : stations) {
        cnt[kv.first] = kv.second.count;
        tsum[kv.first] = kv.second.temp_sum.value();
        rsum[kv.first] = kv.second.rain_sum.value();
    }
    rep.top = wx::top_stations(cnt, tsum, rsum, K);
    wx::pick_busiest(intervals, rep.busy_interval, rep.busy_count);
    wx::write_report(stdout, rep);
    return 0;
}
