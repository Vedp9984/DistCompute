// Shared bits for the SSSP Hadoop-Streaming programs.
//
// Vertex record (one line per vertex, the unit that flows between iterations):
//
//     node \t dist \t flag \t v1:w1,v2:w2,...
//
//   dist  -1 means "not yet reached" (INF); otherwise the best distance so far
//   flag  1 if dist changed in the previous iteration (the node is on the
//         frontier and must push its distance to its neighbours), else 0
//   adj   out-edges; empty for a sink
//
// Intermediate key/value pairs emitted by the mapper:
//
//     node \t N \t dist \t flag \t adj      the vertex record itself, carried forward
//     node \t D \t candidate                 a tentative distance from a neighbour
#ifndef SSSP_COMMON_HPP
#define SSSP_COMMON_HPP

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

namespace sssp {

constexpr long long INF = -1;

struct Record {
    long long node = 0;
    long long dist = INF;
    int flag = 0;
    std::string adj;
};

// Split on tabs, keeping empty trailing fields (an empty adjacency list is a
// legitimate 4th field).
inline std::vector<std::string> split_tabs(const std::string& line) {
    std::vector<std::string> f;
    size_t start = 0;
    while (true) {
        size_t tab = line.find('\t', start);
        if (tab == std::string::npos) { f.push_back(line.substr(start)); break; }
        f.push_back(line.substr(start, tab - start));
        start = tab + 1;
    }
    return f;
}

inline bool parse_record(const std::string& line, Record& r) {
    std::vector<std::string> f = split_tabs(line);
    if (f.size() < 3) return false;
    r.node = std::atoll(f[0].c_str());
    r.dist = std::atoll(f[1].c_str());
    r.flag = std::atoi(f[2].c_str());
    r.adj = f.size() >= 4 ? f[3] : "";
    return true;
}

inline void write_record(const Record& r) {
    std::printf("%lld\t%lld\t%d\t%s\n", r.node, r.dist, r.flag, r.adj.c_str());
}

// Strip a trailing '\r' or '\n'.
inline void chomp(std::string& s) {
    while (!s.empty() && (s.back() == '\n' || s.back() == '\r')) s.pop_back();
}

inline bool getline_fast(std::string& out) {
    static char buf[1 << 16];
    out.clear();
    while (std::fgets(buf, sizeof buf, stdin)) {
        out += buf;
        if (!out.empty() && out.back() == '\n') break;
    }
    if (out.empty()) return false;
    chomp(out);
    return true;
}

}  // namespace sssp

#endif
