// Weather analytics, Hadoop Streaming mapper.
//
// In-mapper combining: every record of the split is folded into one local
// wx::Accum (the same accumulator the HW2 sequential and MPI programs use), and
// only at end-of-input does the mapper emit its partials -- one G line, one
// S:<id> line per station seen, one I:<bucket> line per interval seen.  So the
// shuffle carries O(S + R) lines per mapper instead of O(records): for the
// 1-million-record "small" dataset that is ~2 000 lines instead of 1 000 000.
//
// The header line "N K S" has three fields, a record has seven; the mapper
// whose split contains it forwards it as an H line so the final stage learns K.
#include "wx_partial.hpp"

int main() {
    wx::Accum acc;
    std::string line;
    long long hN = -1, hK = -1, hS = -1;
    while (wxmr::read_line(line)) {
        if (line.empty()) continue;
        long long ts, station;
        double temp, hum, pres, rain, wind;
        // parse_line parks a NUL at line end, so give it a writable buffer.
        if (wx::parse_line(&line[0], &line[0] + line.size(), ts, station,
                           temp, hum, pres, rain, wind)) {
            acc.add(ts, station, temp, hum, pres, rain, wind);
        } else if (hN < 0) {
            long long a, b, c;
            if (std::sscanf(line.c_str(), "%lld %lld %lld", &a, &b, &c) == 3) {
                hN = a; hK = b; hS = c;
            }
        }
    }
    if (hN >= 0) std::printf("H\t%lld %lld %lld\n", hN, hK, hS);
    if (acc.count == 0) return 0;

    wxmr::Global::from_accum(acc).write(stdout);
    for (size_t i = 0; i < acc.st_count.size(); ++i) {
        if (acc.st_count[i] == 0) continue;
        wxmr::StationPartial s;
        s.count = acc.st_count[i]; s.temp_sum = acc.st_temp_sum[i]; s.rain_sum = acc.st_rain_sum[i];
        wxmr::write_station(stdout, static_cast<long long>(i), s);
    }
    for (const auto& kv : acc.intervals) wxmr::write_interval(stdout, kv.first, kv.second);
    return 0;
}
