// Final stage reducer: identity.  Records arrive as "node \t dist" already in
// numeric key order (one reducer, KeyFieldBasedComparator -n); the job sets
// mapreduce.output.textoutputformat.separator to a single space so the file
// reads "node dist" exactly as the assignment specifies.
#include "sssp_common.hpp"

int main() {
    std::string line;
    while (sssp::getline_fast(line)) std::printf("%s\n", line.c_str());
    return 0;
}
