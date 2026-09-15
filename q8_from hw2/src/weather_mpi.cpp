// Q8 -- distributed weather / environmental data analytics.
//
//   mpirun -np P ./weather_mpi INPUT [--output FILE] [--time]
//
// Two things make this scale rather than merely parallelise:
//
//  1. **Parallel input.**  Rank 0 never reads the whole file.  Each rank
//     reads its own byte range with plain POSIX I/O and snaps the ends to line
//     boundaries, so I/O bandwidth scales with P too.  With a rank-0-reads-and-
//     scatters design the serial read would dominate every run and the speedup
//     curve would flatten immediately -- which is exactly the effect the
//     analysis section is meant to discuss.
//
//  2. **Constant-size reductions.**  Everything except the per-station table
//     and the interval histogram reduces to a fixed number of scalars, so the
//     communication cost is independent of N.
//
// stdout carries only the report.  Timing goes to stderr under --time.
#include <mpi.h>

#include <algorithm>
#include <climits>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>

#include "weather_common.hpp"

namespace {

// Streaming granularity for the per-rank read.  Large enough that the read
// syscall cost is amortised, small enough that peak memory stays flat no matter
// how big the input is.
constexpr long long kChunk = 8 << 20;

// Above this many buckets the dense interval reduction would cost more memory
// than it is worth (8M buckets = 64 MB of long long per rank), so the sparse
// path takes over.  8M buckets is ~15 years of wall-clock at 60 s each.
constexpr long long kDenseIntervalLimit = 8LL << 20;

void usage(int rank, const char* prog) {
    if (rank == 0)
        std::fprintf(stderr,
                     "usage: mpirun -np P %s INPUT [--output FILE] [--time]\n",
                     prog);
}

}  // namespace

int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);
    int rank = 0, P = 1;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &P);

    std::string input, output;
    bool timing = false;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--output" && i + 1 < argc) output = argv[++i];
        else if (arg == "--time") timing = true;
        else if (arg == "--input" && i + 1 < argc) input = argv[++i];
        else if (arg[0] != '-' && input.empty()) input = arg;
        else { usage(rank, argv[0]); MPI_Finalize(); return 2; }
    }
    if (input.empty()) { usage(rank, argv[0]); MPI_Finalize(); return 2; }

    const double t_start = MPI_Wtime();

    // ---------------------------------------------------------------------
    // Open the file and read the header.
    //
    // Deliberately plain POSIX I/O rather than MPI-IO.  MPI_File_read_at looks
    // like the natural choice here and works fine on a local disk, but ROMIO
    // (the MPI-IO layer) needs file locking on a network filesystem, and where
    // that locking is unavailable or misconfigured -- the normal situation on an
    // NFS-mounted cluster home directory -- concurrent readers block
    // indefinitely.  That is not hypothetical: it deadlocked every run on the
    // cluster at P = 4 while passing on a laptop.
    //
    // Nothing is lost by dropping MPI-IO.  The parallelism here comes from each
    // rank reading its *own byte range*, which pread does exactly as well; MPI-IO
    // earns its keep for shared-file *writes* and collective access patterns,
    // neither of which this program has.
    // ---------------------------------------------------------------------
    std::FILE* fh = std::fopen(input.c_str(), "rb");
    if (!fh) {
        if (rank == 0)
            std::fprintf(stderr, "error: cannot open %s\n", input.c_str());
        MPI_Finalize();
        return 1;
    }

    std::fseek(fh, 0, SEEK_END);
    const long long fsize = static_cast<long long>(std::ftell(fh));
    std::rewind(fh);

    long long meta[4] = {0, 0, 0, 0};    // N, K, S, header_bytes
    if (rank == 0) {
        std::vector<char> head(256);
        const size_t want = static_cast<size_t>(
            std::min<long long>(static_cast<long long>(head.size()), fsize));
        const size_t got = std::fread(head.data(), 1, want, fh);
        long long N = 0, K = 0, S = 0;
        size_t hb = 0;
        if (!wx::parse_header(head.data(), got, N, K, S, hb)) {
            std::fprintf(stderr, "error: malformed header, expected: N K S\n");
            meta[0] = -1;
        } else {
            meta[0] = N; meta[1] = K; meta[2] = S;
            meta[3] = static_cast<long long>(hb);
        }
    }
    MPI_Bcast(meta, 4, MPI_LONG_LONG, 0, MPI_COMM_WORLD);
    if (meta[0] < 0) { std::fclose(fh); MPI_Finalize(); return 1; }

    const long long K = meta[1], S = meta[2];
    const long long header_bytes = meta[3];

    // ---------------------------------------------------------------------
    // Split the record region into P byte ranges and stream one each.
    //
    // Ownership rule: a record belongs to the rank whose range contains its
    // *first* byte.  Rank 0's range starts exactly at the first record; every
    // other rank begins mid-record and therefore discards everything up to and
    // including its first newline.  The rank before it keeps reading past its
    // own end until that record's newline, so the record is picked up exactly
    // once -- no communication needed to agree on the boundary.
    //
    // The range is streamed in 8 MB chunks rather than read in one go.  Reading
    // a whole range at once would make each rank's peak memory O(file / P),
    // which puts a ceiling on the input size that has nothing to do with the
    // algorithm; streaming makes it O(1).
    // ---------------------------------------------------------------------
    const long long data_bytes = (fsize > header_bytes) ? fsize - header_bytes : 0;
    const long long raw_start = header_bytes + data_bytes * rank / P;
    const long long raw_end = header_bytes + data_bytes * (rank + 1) / P;

    wx::Accum acc;
    acc.reserve_stations(S);

    double read_secs = 0.0;
    {
        std::vector<char> chunk(static_cast<size_t>(kChunk));
        std::string carry;                 // partial record from the last chunk
        long long carry_at = raw_start;    // file offset of carry's first byte
        long long pos = raw_start;
        bool done = false;

        // Does this rank's range begin part-way through a record?  Only then
        // must the first record be dropped, because only then does it belong to
        // the previous rank.  If raw_start happens to land *exactly* on a
        // record boundary the record is ours: the previous rank stops at
        // `line_at >= raw_end`, which is this same offset, so dropping it here
        // too would lose it entirely.  One byte of look-back settles it.
        bool drop_first = false;
        if (rank != 0 && raw_start > 0) {
            char prev = '\n';
            std::fseek(fh, static_cast<long>(raw_start - 1), SEEK_SET);
            if (std::fread(&prev, 1, 1, fh) == 1) drop_first = (prev != '\n');
        }

        std::fseek(fh, static_cast<long>(raw_start), SEEK_SET);

        while (!done && pos < fsize) {
            const size_t want = static_cast<size_t>(
                std::min<long long>(kChunk, fsize - pos));

            const double r0 = MPI_Wtime();
            // fread reports what it actually delivered, so a short read on a
            // busy filesystem truncates this chunk rather than silently
            // parsing whatever was left in the buffer.
            const size_t got = std::fread(chunk.data(), 1, want, fh);
            read_secs += MPI_Wtime() - r0;
            if (got == 0) break;           // EOF or error: nothing more to parse

            std::string work;
            work.swap(carry);              // leaves carry empty
            const long long work_at = carry_at;
            work.append(chunk.data(), got);
            pos += static_cast<long long>(got);

            size_t i = 0;
            while (true) {
                const char* nl = static_cast<const char*>(
                    std::memchr(work.data() + i, '\n', work.size() - i));
                if (!nl) break;
                const size_t e = static_cast<size_t>(nl - work.data());
                const long long line_at = work_at + static_cast<long long>(i);

                if (drop_first) {
                    drop_first = false;    // this record belongs to rank-1
                } else if (line_at >= raw_end) {
                    done = true;           // this record belongs to rank+1
                    break;
                } else {
                    wx::parse_buffer(&work[i], &work[e], acc);
                }
                i = e + 1;
            }
            if (done) break;
            carry.assign(work, i, std::string::npos);
            carry_at = work_at + static_cast<long long>(i);
        }

        // A final record with no trailing newline at end of file.
        if (!done && !drop_first && !carry.empty() && carry_at < raw_end) {
            carry.push_back('\n');         // keep parse_line inside real bytes
            wx::parse_buffer(&carry[0], &carry[carry.size() - 1], acc);
        }
    }
    std::fclose(fh);

    const double t_parse = MPI_Wtime();

    // ---------------------------------------------------------------------
    // Reduce the scalars.
    // ---------------------------------------------------------------------
    const double t_reduce0 = MPI_Wtime();

    long long lcount[2] = {acc.count, acc.extreme};
    long long gcount[2] = {0, 0};
    MPI_Reduce(lcount, gcount, 2, MPI_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);

    double lsum[5] = {acc.sum_t.value(), acc.sum_h.value(), acc.sum_p.value(),
                      acc.sum_rain.value(), acc.sum_wind.value()};
    double gsum[5] = {0, 0, 0, 0, 0};
    MPI_Reduce(lsum, gsum, 5, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);

    double lmin[3] = {acc.min_t, acc.min_h, acc.min_p};
    double gmin[3];
    MPI_Reduce(lmin, gmin, 3, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);

    double lmax[5] = {acc.max_t, acc.max_h, acc.max_p, acc.max_rain,
                      acc.max_wind};
    double gmax[5];
    MPI_Reduce(lmax, gmax, 5, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

    // ---------------------------------------------------------------------
    // Hottest / coldest.
    //
    // MPI_MAXLOC pairs a value with a single index and breaks ties on that
    // index alone; the assignment wants temperature, then timestamp, then
    // station.  So each rank contributes one candidate and rank 0 applies the
    // real comparator across the P of them -- P is tiny, this costs nothing.
    // ---------------------------------------------------------------------
    double loc_t[2] = {acc.hottest.temp, acc.coldest.temp};
    long long loc_i[5] = {acc.hottest.station, acc.hottest.ts,
                          acc.coldest.station, acc.coldest.ts,
                          acc.hottest.valid ? 1 : 0};
    std::vector<double> all_t(rank == 0 ? 2 * P : 0);
    std::vector<long long> all_i(rank == 0 ? 5 * P : 0);
    MPI_Gather(loc_t, 2, MPI_DOUBLE, all_t.data(), 2, MPI_DOUBLE, 0,
               MPI_COMM_WORLD);
    MPI_Gather(loc_i, 5, MPI_LONG_LONG, all_i.data(), 5, MPI_LONG_LONG, 0,
               MPI_COMM_WORLD);

    // ---------------------------------------------------------------------
    // Per-station table.
    //
    // Station ids beyond the declared S are tolerated: take the global maximum
    // first, then reduce dense arrays of that length.
    // ---------------------------------------------------------------------
    long long local_span = static_cast<long long>(acc.st_count.size());
    long long span = 0;
    MPI_Allreduce(&local_span, &span, 1, MPI_LONG_LONG, MPI_MAX,
                  MPI_COMM_WORLD);

    acc.st_count.resize(static_cast<size_t>(span), 0);
    acc.st_temp_sum.resize(static_cast<size_t>(span));
    acc.st_rain_sum.resize(static_cast<size_t>(span));

    // Flatten the compensated accumulators into contiguous doubles: MPI_Reduce
    // sums plain arrays, and a KSum is a pair.
    const std::vector<double> l_st_temp = wx::materialize(acc.st_temp_sum);
    const std::vector<double> l_st_rain = wx::materialize(acc.st_rain_sum);

    std::vector<long long> g_st_count(rank == 0 ? static_cast<size_t>(span) : 0);
    std::vector<double> g_st_temp(rank == 0 ? static_cast<size_t>(span) : 0);
    std::vector<double> g_st_rain(rank == 0 ? static_cast<size_t>(span) : 0);
    if (span > 0) {
        MPI_Reduce(acc.st_count.data(), g_st_count.data(),
                   static_cast<int>(span), MPI_LONG_LONG, MPI_SUM, 0,
                   MPI_COMM_WORLD);
        MPI_Reduce(l_st_temp.data(), g_st_temp.data(),
                   static_cast<int>(span), MPI_DOUBLE, MPI_SUM, 0,
                   MPI_COMM_WORLD);
        MPI_Reduce(l_st_rain.data(), g_st_rain.data(),
                   static_cast<int>(span), MPI_DOUBLE, MPI_SUM, 0,
                   MPI_COMM_WORLD);
    }

    // ---------------------------------------------------------------------
    // Busiest interval.
    //
    // Dense reduction when the global span of bucket ids is small enough to
    // materialise; otherwise gather the sparse (bucket, count) pairs and
    // aggregate on rank 0.  Real inputs almost always take the dense path.
    // ---------------------------------------------------------------------
    long long lo = acc.intervals.empty() ? 0
                                         : acc.intervals.begin()->first;
    long long hi = lo;
    bool have_any = !acc.intervals.empty();
    for (const auto& kv : acc.intervals) {
        lo = std::min(lo, kv.first);
        hi = std::max(hi, kv.first);
    }
    if (!have_any) { lo = LLONG_MAX; hi = LLONG_MIN; }

    long long glo = 0, ghi = 0;
    MPI_Allreduce(&lo, &glo, 1, MPI_LONG_LONG, MPI_MIN, MPI_COMM_WORLD);
    MPI_Allreduce(&hi, &ghi, 1, MPI_LONG_LONG, MPI_MAX, MPI_COMM_WORLD);

    long long busy_interval = 0, busy_count = 0;
    const bool any_records = (ghi >= glo);
    const long long range = any_records ? (ghi - glo + 1) : 0;

    if (any_records && range <= kDenseIntervalLimit) {
        std::vector<long long> dense(static_cast<size_t>(range), 0);
        for (const auto& kv : acc.intervals)
            dense[static_cast<size_t>(kv.first - glo)] += kv.second;

        std::vector<long long> gdense(rank == 0 ? static_cast<size_t>(range) : 0);
        MPI_Reduce(dense.data(), gdense.data(), static_cast<int>(range),
                   MPI_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
        if (rank == 0) {
            for (long long i = 0; i < range; ++i) {
                // Strict > keeps the *first*, i.e. smallest, id on a tie.
                if (gdense[static_cast<size_t>(i)] > busy_count) {
                    busy_count = gdense[static_cast<size_t>(i)];
                    busy_interval = glo + i;
                }
            }
        }
    } else if (any_records) {
        // Sparse fallback: ship only the buckets that occur.
        std::vector<long long> pairs;
        pairs.reserve(acc.intervals.size() * 2);
        for (const auto& kv : acc.intervals) {
            pairs.push_back(kv.first);
            pairs.push_back(kv.second);
        }
        int mine = static_cast<int>(pairs.size());
        std::vector<int> counts(rank == 0 ? P : 0), displs(rank == 0 ? P : 0);
        MPI_Gather(&mine, 1, MPI_INT, counts.data(), 1, MPI_INT, 0,
                   MPI_COMM_WORLD);
        long long total = 0;
        if (rank == 0) {
            for (int r = 0; r < P; ++r) { displs[r] = static_cast<int>(total);
                                          total += counts[r]; }
        }
        std::vector<long long> all(rank == 0 ? static_cast<size_t>(total) : 0);
        MPI_Gatherv(pairs.data(), mine, MPI_LONG_LONG, all.data(),
                    counts.data(), displs.data(), MPI_LONG_LONG, 0,
                    MPI_COMM_WORLD);
        if (rank == 0) {
            std::unordered_map<long long, long long> merged;
            for (size_t i = 0; i + 1 < all.size(); i += 2)
                merged[all[i]] += all[i + 1];
            wx::pick_busiest(merged, busy_interval, busy_count);
        }
    }

    const double t_reduce1 = MPI_Wtime();

    // ---------------------------------------------------------------------
    // Rank 0 assembles and writes the report.
    // ---------------------------------------------------------------------
    if (rank == 0) {
        wx::Report rep;
        wx::finalize(rep, gcount[0], gsum[0], gsum[1], gsum[2], gsum[3], gsum[4],
                     gmin[0], gmax[0], gmin[1], gmax[1], gmin[2], gmax[2],
                     gmax[3], gmax[4], gcount[1]);

        for (int r = 0; r < P; ++r) {
            wx::Extremum hot{all_t[2 * r], all_i[5 * r], all_i[5 * r + 1],
                             all_i[5 * r + 4] != 0};
            wx::Extremum cold{all_t[2 * r + 1], all_i[5 * r + 2],
                              all_i[5 * r + 3], all_i[5 * r + 4] != 0};
            if (wx::hotter(hot, rep.hottest)) rep.hottest = hot;
            if (wx::colder(cold, rep.coldest)) rep.coldest = cold;
        }

        rep.top = wx::top_stations(g_st_count, g_st_temp, g_st_rain, K);
        rep.busy_interval = busy_interval;
        rep.busy_count = busy_count;

        std::FILE* out = stdout;
        if (!output.empty()) {
            out = std::fopen(output.c_str(), "w");
            if (!out) {
                std::fprintf(stderr, "error: cannot write %s\n",
                             output.c_str());
                MPI_Abort(MPI_COMM_WORLD, 1);
            }
        }
        wx::write_report(out, rep);
        if (out != stdout) std::fclose(out);
    }

    if (timing) {
        // Read and parse are interleaved by the streaming loop, so they are
        // separated by accumulating the time actually spent inside
        // the read calls rather than by wall-clock phase boundaries.
        double local_t[4] = {read_secs,                            // I/O
                             (t_parse - t_start) - read_secs,      // parsing
                             t_reduce1 - t_reduce0,                // collectives
                             MPI_Wtime() - t_start};               // total
        double worst[4];
        MPI_Reduce(local_t, worst, 4, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
        if (rank == 0) {
            std::fprintf(stderr,
                "mpi P=%d N=%lld read=%.6f parse=%.6f reduce=%.6f total=%.6f\n",
                P, gcount[0], worst[0], worst[1], worst[2], worst[3]);
        }
    }

    MPI_Finalize();
    return 0;
}
