#!/bin/bash
# Q8 benchmark dataset generation.
#
# Every dataset is a pure function of (N, S, K, seed, span), so this script is
# the whole reproduction procedure: run it with the same SEED and you get
# byte-identical files.  Datasets are regenerated on the cluster rather than
# uploaded (see ../sync.sh), which is why reproducibility matters here.
#
#   bash scripts/gen_data.sh            # the four benchmark sizes
#   SEED=99 bash scripts/gen_data.sh    # a different reproducible draw
set -euo pipefail
cd "$(dirname "$0")/.."

BIN=${BIN:-bin}
SEED=${SEED:-2024}
K=${K:-10}
mkdir -p data

# label  N          S      span (seconds)
#
# S grows with N so the per-station table stays meaningful rather than
# collapsing to a handful of enormous stations.  The span is widened with N so
# the 60-second interval histogram keeps a comparable number of buckets --
# otherwise the "busiest interval" result would degenerate to "all of them".
SETS="
small   1000000    500    86400
medium  5000000    2000   259200
large   20000000   5000   604800
xlarge  50000000   10000  1209600
"

echo "$SETS" | while read -r label N S span; do
    [ -z "$label" ] && continue
    out="data/weather_${label}.txt"
    if [ -f "$out" ]; then
        printf '  exists  %-8s %s\n' "$label" "$out"
        continue
    fi
    printf '  writing %-8s N=%-10s S=%-6s span=%ss\n' "$label" "$N" "$S" "$span"
    $BIN/gen_weather "$N" "$S" "$K" --seed "$SEED" --span "$span" --output "$out"
done

echo
ls -lh data/
