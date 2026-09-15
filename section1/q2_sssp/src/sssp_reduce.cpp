// Iteration reducer.
//
// For each vertex: new_dist = min(current dist, min candidate).  Emits the
// updated record with flag=1 iff the distance improved, and bumps the Hadoop
// counter SSSP.UPDATED so the driver can detect convergence (a whole
// iteration with UPDATED == 0 means no distance changed anywhere).
#include "sssp_common.hpp"

int main() {
    std::string line, cur;
    sssp::Record rec;
    bool have_rec = false;
    long long best = sssp::INF;
    long long updated = 0;

    auto flush = [&]() {
        if (cur.empty()) return;
        if (!have_rec) {                     // cannot happen after prep, but be safe
            rec = sssp::Record{};
            rec.node = std::atoll(cur.c_str());
        }
        bool improved = best != sssp::INF && (rec.dist == sssp::INF || best < rec.dist);
        if (improved) { rec.dist = best; ++updated; }
        rec.flag = improved ? 1 : 0;
        sssp::write_record(rec);
        have_rec = false; best = sssp::INF;
    };

    while (sssp::getline_fast(line)) {
        std::vector<std::string> f = sssp::split_tabs(line);
        if (f.size() < 3) continue;
        if (f[0] != cur) { flush(); cur = f[0]; }
        if (f[1] == "N" && f.size() >= 4) {
            rec.node = std::atoll(f[0].c_str());
            rec.dist = std::atoll(f[2].c_str());
            rec.flag = std::atoi(f[3].c_str());
            rec.adj = f.size() >= 5 ? f[4] : "";
            have_rec = true;
        } else if (f[1] == "D") {
            long long d = std::atoll(f[2].c_str());
            if (best == sssp::INF || d < best) best = d;
        }
    }
    flush();
    // Hadoop Streaming picks counter updates off stderr.
    if (updated > 0) std::fprintf(stderr, "reporter:counter:SSSP,UPDATED,%lld\n", updated);
    return 0;
}
