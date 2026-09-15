// Final stage mapper: vertex record -> "node \t dist|INF".  The job runs with
// a numeric key comparator and one reducer, so the output is sorted by node id.
#include "sssp_common.hpp"

int main() {
    std::string line;
    sssp::Record r;
    while (sssp::getline_fast(line)) {
        if (!sssp::parse_record(line, r)) continue;
        if (r.dist == sssp::INF) std::printf("%lld\tINF\n", r.node);
        else                     std::printf("%lld\t%lld\n", r.node, r.dist);
    }
    return 0;
}
