// Weather analytics, Hadoop Streaming combiner / stage-1 reducer.
//
// Input is grouped by key.  Lines with the same key are merged -- sums added
// (compensated), min/max taken, hottest/coldest resolved with the assignment's
// tie-break -- and re-emitted in the identical format.  Because input and
// output formats coincide the program is idempotent under composition, which
// is exactly the property Hadoop requires of a combiner.
#include "wx_partial.hpp"

int main() {
    std::string line, cur;
    char kind = 0;
    wxmr::Global g;
    wxmr::StationPartial s;
    long long icount = 0;
    std::string header;

    auto flush = [&]() {
        if (cur.empty()) return;
        switch (kind) {
            case 'G': g.write(stdout); break;
            case 'S': wxmr::write_station(stdout, std::atoll(cur.c_str() + 2), s); break;
            case 'I': wxmr::write_interval(stdout, std::atoll(cur.c_str() + 2), icount); break;
            case 'H': std::printf("H\t%s\n", header.c_str()); break;
        }
        g = wxmr::Global{}; s = wxmr::StationPartial{}; icount = 0; header.clear();
    };

    while (wxmr::read_line(line)) {
        size_t tab = line.find('\t');
        if (tab == std::string::npos) continue;
        if (line.compare(0, tab, cur) != 0) {
            flush();
            cur = line.substr(0, tab);
            kind = cur[0];
        }
        const char* v = line.c_str() + tab + 1;
        switch (kind) {
            case 'G': { wxmr::Global o; o.parse(v); g.merge_from(o); break; }
            case 'S': {
                char* p = const_cast<char*>(v);
                s.count += std::strtoll(p, &p, 10);
                s.temp_sum.add(std::strtod(p, &p));
                s.rain_sum.add(std::strtod(p, &p));
                break;
            }
            case 'I': icount += std::atoll(v); break;
            case 'H': if (header.empty()) header = v; break;
        }
    }
    flush();
    return 0;
}
