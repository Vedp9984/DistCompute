// Q8 -- sequential weather / environmental data analytics.
//
// The correctness oracle and the T_1 baseline for the speedup tables.
//
//   ./weather_seq INPUT [--output FILE] [--time]
//
// The file is streamed in 8 MB blocks with a carry-over for the line straddling
// a block boundary, so a multi-gigabyte input never has to fit in memory.
#include <chrono>
#include <cstdio>
#include <string>
#include <vector>

#include "weather_common.hpp"

namespace {
constexpr size_t kBlock = 8u << 20;

void usage(const char* prog) {
    std::fprintf(stderr, "usage: %s INPUT [--output FILE] [--time]\n", prog);
}
}  // namespace

int main(int argc, char** argv) {
    std::string input, output;
    bool timing = false;

    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--output" && i + 1 < argc) output = argv[++i];
        else if (arg == "--time") timing = true;
        else if (arg == "--input" && i + 1 < argc) input = argv[++i];
        else if (arg[0] != '-' && input.empty()) input = arg;
        else { usage(argv[0]); return 2; }
    }
    if (input.empty()) { usage(argv[0]); return 2; }

    std::FILE* f = std::fopen(input.c_str(), "rb");
    if (!f) {
        std::fprintf(stderr, "error: cannot open %s\n", input.c_str());
        return 1;
    }

    const auto t0 = std::chrono::steady_clock::now();

    // ---- header -----------------------------------------------------------
    std::vector<char> head(256);
    const size_t got = std::fread(head.data(), 1, head.size(), f);
    long long N = 0, K = 0, S = 0;
    size_t header_bytes = 0;
    if (!wx::parse_header(head.data(), got, N, K, S, header_bytes)) {
        std::fprintf(stderr, "error: malformed header, expected: N K S\n");
        std::fclose(f);
        return 1;
    }
    std::fseek(f, static_cast<long>(header_bytes), SEEK_SET);

    // ---- records ----------------------------------------------------------
    wx::Accum acc;
    acc.reserve_stations(S);

    std::vector<char> buf(kBlock);
    std::string carry;                    // partial line from the previous block
    while (true) {
        const size_t n = std::fread(buf.data(), 1, kBlock, f);
        if (n == 0) break;

        // Everything up to the last newline in this block is complete lines;
        // the tail becomes the carry for the next iteration.
        size_t last_nl = n;
        while (last_nl > 0 && buf[last_nl - 1] != '\n') --last_nl;

        if (last_nl == 0) {               // no line ended inside this block
            carry.append(buf.data(), n);
            continue;
        }
        if (!carry.empty()) {
            carry.append(buf.data(), last_nl);
            wx::parse_buffer(carry.data(), carry.data() + carry.size(), acc);
            carry.clear();
        } else {
            wx::parse_buffer(buf.data(), buf.data() + last_nl, acc);
        }
        carry.assign(buf.data() + last_nl, n - last_nl);
    }
    // Final line with no trailing newline.  The '\n' is appended so that
    // parse_line's temporary NUL lands on a real character rather than on the
    // string's terminator.
    if (!carry.empty()) {
        carry.push_back('\n');
        wx::parse_buffer(&carry[0], &carry[carry.size() - 1], acc);
    }
    std::fclose(f);

    const auto t1 = std::chrono::steady_clock::now();

    // ---- report -----------------------------------------------------------
    wx::Report rep;
    wx::finalize(rep, acc.count, acc.sum_t.value(), acc.sum_h.value(),
                 acc.sum_p.value(), acc.sum_rain.value(), acc.sum_wind.value(),
                 acc.min_t, acc.max_t, acc.min_h, acc.max_h, acc.min_p,
                 acc.max_p, acc.max_rain, acc.max_wind, acc.extreme);
    rep.hottest = acc.hottest;
    rep.coldest = acc.coldest;
    rep.top = wx::top_stations(acc.st_count, wx::materialize(acc.st_temp_sum),
                               wx::materialize(acc.st_rain_sum), K);

    wx::pick_busiest(acc.intervals, rep.busy_interval, rep.busy_count);

    const auto t2 = std::chrono::steady_clock::now();

    if (timing) {
        std::fprintf(stderr, "seq N=%lld parse=%.6f reduce=%.6f total=%.6f\n",
                     acc.count,
                     std::chrono::duration<double>(t1 - t0).count(),
                     std::chrono::duration<double>(t2 - t1).count(),
                     std::chrono::duration<double>(t2 - t0).count());
    }

    std::FILE* out = stdout;
    if (!output.empty()) {
        out = std::fopen(output.c_str(), "w");
        if (!out) {
            std::fprintf(stderr, "error: cannot write %s\n", output.c_str());
            return 1;
        }
    }
    wx::write_report(out, rep);
    if (out != stdout) std::fclose(out);
    return 0;
}
