#!/bin/bash
# smoke test workload: build, run SSSP sample + weather handmade on the cluster
set -x
cd ~/HW3
module load python/3.12.5 2>/dev/null
make -C section1/q2_sssp -s && make -C section2/q1_mapreduce -s || { echo BUILD_FAILED; exit 1; }
(cd section1/q2_sssp && bash run_hadoop.sh tests/sample.txt results/sample_cluster.out --reducers 2 --maps 2 && cat results/sample_cluster.out && python3 oracle.py < tests/sample.txt | diff - results/sample_cluster.out && echo SSSP_OK)
(cd section2/q1_mapreduce && bash run_hadoop.sh tests/handmade.txt results/handmade_cluster.out --maps 2 --reducers 2 && diff results/handmade_cluster.out tests/handmade.expected && echo WX_OK)
