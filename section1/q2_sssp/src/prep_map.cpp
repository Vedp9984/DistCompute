// Prep stage, mapper: edge list -> per-vertex fragments.
//
// Input lines are either the header "V E" (2 fields) or an edge "u v w".
// For an edge we emit    u \t E \t v:w        (an out-edge of u)
//                        v \t V               (v exists, even if it has no out-edges)
// For the header we emit i \t V  for i in [0, V), so every vertex 0..V-1 gets a
// record and appears in the final output even when it touches no edge at all.
#include "sssp_common.hpp"

int main() {
    std::string line;
    while (sssp::getline_fast(line)) {
        long long a, b, c;
        int n = std::sscanf(line.c_str(), "%lld %lld %lld", &a, &b, &c);
        if (n == 2) {                                // header: V E
            for (long long i = 0; i < a; ++i) std::printf("%lld\tV\n", i);
        } else if (n == 3) {                         // edge: u v w
            std::printf("%lld\tE\t%lld:%lld\n", a, b, c);
            std::printf("%lld\tV\n", b);
        }
    }
    return 0;
}
