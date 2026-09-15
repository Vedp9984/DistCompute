#!/bin/bash
# Fetch results, demo transcripts and job logs back from the cluster.
mkdir -p logs/cluster section1/q2_sssp/results/cluster section2/q1_mapreduce/results/cluster section2/q2_grpc/results/cluster section3/problem1/demo
R=cs3401.20@rce.iiit.ac.in:HW3
rsync -az $R/section1/q2_sssp/results/ section1/q2_sssp/results/cluster/
rsync -az $R/section2/q1_mapreduce/results/ section2/q1_mapreduce/results/cluster/
rsync -az $R/section2/q2_grpc/results/ section2/q2_grpc/results/cluster/
rsync -az $R/section3/problem1/demo/ section3/problem1/demo/
rsync -az $R/logs/ logs/cluster/
