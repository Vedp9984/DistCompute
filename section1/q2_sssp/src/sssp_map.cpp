// Iteration mapper.
//
// Every vertex record is passed through unchanged (key N) so the graph
// structure survives the iteration.  A reached vertex additionally pushes
// dist + w to each out-neighbour (key D).
//
// SSSP_MODE=frontier (default): only vertices whose distance changed in the
//   previous iteration push -- the classic frontier optimisation.  Correct
//   because distances only ever decrease and every decrease is pushed exactly
//   once, in the very next iteration.
// SSSP_MODE=full: every reached vertex pushes every iteration (textbook
//   Bellman-Ford-in-MapReduce).  Same answer, far more intermediate data;
//   kept for the benchmark comparison.
#include "sssp_common.hpp"

int main() {
    const char* mode = std::getenv("SSSP_MODE");
    const bool frontier_only = !(mode && std::strcmp(mode, "full") == 0);

    std::string line;
    sssp::Record r;
    while (sssp::getline_fast(line)) {
        if (!sssp::parse_record(line, r)) continue;
        std::printf("%lld\tN\t%lld\t%d\t%s\n", r.node, r.dist, r.flag, r.adj.c_str());

        if (r.dist == sssp::INF) continue;
        if (frontier_only && !r.flag) continue;

        // adj = "v:w,v:w,..."
        const char* p = r.adj.c_str();
        while (*p) {
            char* q;
            long long v = std::strtoll(p, &q, 10);
            if (q == p || *q != ':') break;
            long long w = std::strtoll(q + 1, &q, 10);
            std::printf("%lld\tD\t%lld\n", v, r.dist + w);
            if (*q == ',') ++q;
            p = q;
        }
    }
    return 0;
}
