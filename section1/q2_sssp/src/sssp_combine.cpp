// Iteration combiner: per key keep the minimum candidate distance; pass the
// vertex record through untouched.  Cuts shuffle volume roughly by the
// average in-degree.
#include "sssp_common.hpp"

int main() {
    std::string line, cur, nrec;
    long long best = sssp::INF;
    auto flush = [&]() {
        if (cur.empty()) return;
        if (!nrec.empty()) std::printf("%s\n", nrec.c_str());
        if (best != sssp::INF) std::printf("%s\tD\t%lld\n", cur.c_str(), best);
        nrec.clear(); best = sssp::INF;
    };
    while (sssp::getline_fast(line)) {
        size_t t1 = line.find('\t');
        if (t1 == std::string::npos) continue;
        std::string key = line.substr(0, t1);
        if (key != cur) { flush(); cur = key; }
        if (line.compare(t1 + 1, 1, "N") == 0) {
            nrec = line;
        } else if (line.compare(t1 + 1, 1, "D") == 0) {
            long long d = std::atoll(line.c_str() + t1 + 3);
            if (best == sssp::INF || d < best) best = d;
        }
    }
    flush();
    return 0;
}
