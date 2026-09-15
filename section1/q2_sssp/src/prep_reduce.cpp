// Prep stage, reducer: group the fragments of one vertex into its record.
// Node 0 starts at distance 0 and on the frontier; everyone else at INF.
#include "sssp_common.hpp"

namespace {
void flush(long long node, std::string& adj) {
    sssp::Record r;
    r.node = node;
    r.dist = (node == 0) ? 0 : sssp::INF;
    r.flag = (node == 0) ? 1 : 0;
    r.adj = adj;
    sssp::write_record(r);
    adj.clear();
}
}  // namespace

int main() {
    std::string line, adj;
    long long cur = -1;
    bool have = false;
    while (sssp::getline_fast(line)) {
        std::vector<std::string> f = sssp::split_tabs(line);
        if (f.size() < 2) continue;
        long long node = std::atoll(f[0].c_str());
        if (have && node != cur) flush(cur, adj);
        cur = node; have = true;
        if (f[1] == "E" && f.size() >= 3) {
            if (!adj.empty()) adj += ',';
            adj += f[2];
        }
    }
    if (have) flush(cur, adj);
    return 0;
}
