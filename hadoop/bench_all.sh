#!/bin/bash
# Full Hadoop benchmark workload: Section 1 Q2 (SSSP) + Section 2 Q1 (weather MR vs MPI).
cd ~/HW3
echo "########## SECTION 1 Q2: SSSP ##########"
bash section1/q2_sssp/bench_cluster.sh
echo "########## SECTION 2 Q1: WEATHER MR + MPI ##########"
bash section2/q1_mapreduce/bench_cluster.sh
