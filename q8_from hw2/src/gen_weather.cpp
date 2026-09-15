// Q8 -- reproducible weather dataset generator.
//
//   ./gen_weather N S K [--seed SEED] [--span SECONDS] [--output FILE]
//
// Writes the assignment's input format:
//
//   N K S
//   timestamp station_id temperature humidity pressure rainfall wind_speed
//
// Everything is a pure function of (N, S, K, seed, span), so the same command
// reproduces the same bytes on any machine -- see the note on SplitMix64 below.
// The full generation model is documented in q8/README.md.
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

namespace {

// SplitMix64 rather than <random>: the C++ standard pins down the *engines* but
// not the *distributions*, so std::normal_distribution can and does emit
// different values across standard libraries.  A dataset that differs between
// the laptop and the cluster would make the benchmark numbers incomparable.
class SplitMix64 {
public:
    explicit SplitMix64(uint64_t seed) : state_(seed) {}
    uint64_t next() {
        state_ += 0x9E3779B97F4A7C15ULL;
        uint64_t z = state_;
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
        return z ^ (z >> 31);
    }
    // Uniform in [0,1).  53 bits of mantissa is all a double can hold.
    double uniform() {
        return static_cast<double>(next() >> 11) * (1.0 / 9007199254740992.0);
    }
    double uniform(double lo, double hi) { return lo + (hi - lo) * uniform(); }
    long long integer(long long lo, long long hi) {
        return lo + static_cast<long long>(next() % static_cast<uint64_t>(hi - lo + 1));
    }
    // Box-Muller.  Guard against uniform() returning exactly 0, where log
    // would be -inf.
    double normal(double mean, double sd) {
        double u1 = uniform();
        if (u1 < 1e-300) u1 = 1e-300;
        const double u2 = uniform();
        return mean + sd * std::sqrt(-2.0 * std::log(u1)) *
                          std::cos(6.283185307179586 * u2);
    }

private:
    uint64_t state_;
};

constexpr long long kEpoch = 1700000000LL;   // arbitrary fixed base timestamp

double clampd(double v, double lo, double hi) {
    return v < lo ? lo : (v > hi ? hi : v);
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 4) {
        std::fprintf(stderr,
            "usage: %s N S K [--seed SEED] [--span SECONDS] [--output FILE]\n"
            "  N     number of measurements\n"
            "  S     number of stations (ids 0 .. S-1)\n"
            "  K     top-K parameter written into the header\n", argv[0]);
        return 2;
    }
    const long long N = std::atoll(argv[1]);
    const long long S = std::atoll(argv[2]);
    const long long K = std::atoll(argv[3]);
    unsigned long long seed = 2024;
    long long span = 86400;                  // one day of wall-clock by default
    std::string output;

    for (int i = 4; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--seed" && i + 1 < argc) seed = std::strtoull(argv[++i], nullptr, 10);
        else if (arg == "--span" && i + 1 < argc) span = std::atoll(argv[++i]);
        else if (arg == "--output" && i + 1 < argc) output = argv[++i];
        else { std::fprintf(stderr, "error: unknown argument %s\n", argv[i]); return 2; }
    }
    if (N < 0 || S <= 0 || K < 0 || span <= 0) {
        std::fprintf(stderr, "error: need N >= 0, S > 0, K >= 0, span > 0\n");
        return 2;
    }

    // --- per-station climate, drawn once -----------------------------------
    // Each station gets its own baseline so that averages differ per station
    // and the TOP_STATIONS temperatures are not all the same number.
    SplitMix64 rng(seed);
    std::vector<double> base_temp(static_cast<size_t>(S));
    std::vector<double> base_hum(static_cast<size_t>(S));
    std::vector<double> base_pres(static_cast<size_t>(S));
    std::vector<double> wet(static_cast<size_t>(S));
    for (long long s = 0; s < S; ++s) {
        base_temp[static_cast<size_t>(s)] = rng.uniform(4.0, 34.0);
        base_hum[static_cast<size_t>(s)] = rng.uniform(30.0, 85.0);
        base_pres[static_cast<size_t>(s)] = rng.uniform(985.0, 1025.0);
        wet[static_cast<size_t>(s)] = rng.uniform(0.02, 0.30);  // P(rain)
    }

    std::FILE* out = stdout;
    if (!output.empty()) {
        out = std::fopen(output.c_str(), "w");
        if (!out) {
            std::fprintf(stderr, "error: cannot write %s\n", output.c_str());
            return 1;
        }
    }
    std::fprintf(out, "%lld %lld %lld\n", N, K, S);

    std::string buf;
    buf.reserve(1u << 22);
    char tmp[192];

    for (long long i = 0; i < N; ++i) {
        const long long ts = kEpoch + rng.integer(0, span - 1);

        // Station choice is deliberately skewed, not uniform: with a uniform
        // draw every station gets ~N/S measurements and the top-K ranking is
        // decided by noise, which makes the output useless for checking the
        // tie-break rule.  u^2 concentrates mass on the low ids, so the
        // ranking is stable and reproducible.
        const double u = rng.uniform();
        long long station = static_cast<long long>(static_cast<double>(S) * u * u);
        if (station >= S) station = S - 1;

        // Diurnal cycle plus station baseline plus noise.  The amplitude and
        // the noise width are chosen so a small but non-trivial fraction of
        // readings crosses the >= 40 C / <= 0 C extreme thresholds.
        const double tod = static_cast<double>((ts % 86400LL)) / 86400.0;
        const double diurnal = 7.5 * std::sin(6.283185307179586 * (tod - 0.25));
        const double temp = base_temp[static_cast<size_t>(station)] + diurnal +
                            rng.normal(0.0, 3.5);

        // Humidity moves opposite to temperature, then is clipped to [0,100].
        const double hum = clampd(base_hum[static_cast<size_t>(station)] -
                                      0.8 * diurnal + rng.normal(0.0, 6.0),
                                  0.0, 100.0);
        const double pres = clampd(base_pres[static_cast<size_t>(station)] +
                                       rng.normal(0.0, 4.0),
                                   870.0, 1085.0);

        // Rain is zero most of the time; when it falls it is exponential-ish,
        // which is much closer to real precipitation than a uniform draw.
        double rain = 0.0;
        if (rng.uniform() < wet[static_cast<size_t>(station)]) {
            double u2 = rng.uniform();
            if (u2 < 1e-300) u2 = 1e-300;
            rain = -3.0 * std::log(u2);
        }

        const double wind = clampd(std::fabs(rng.normal(0.0, 7.0)), 0.0, 60.0);

        buf.append(tmp, static_cast<size_t>(std::snprintf(
            tmp, sizeof(tmp), "%lld %lld %.2f %.2f %.2f %.2f %.2f\n",
            ts, station, temp, hum, pres, rain, wind)));

        if (buf.size() > (1u << 21)) {
            std::fwrite(buf.data(), 1, buf.size(), out);
            buf.clear();
        }
    }
    std::fwrite(buf.data(), 1, buf.size(), out);
    if (out != stdout) std::fclose(out);
    return 0;
}
