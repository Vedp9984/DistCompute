#!/bin/bash
# Generate a Hadoop configuration directory for a cluster made of Slurm nodes.
#
#   bash gen_config.sh CONF_DIR MASTER "node01 node02 node03" [VCORES_PER_NODE] [MEM_MB_PER_NODE]
#
# The first node is NameNode + ResourceManager; every node is DataNode +
# NodeManager.  All state goes to node-local /scratch (never the NFS home).
# Ports are moved off the defaults so two students' clusters can coexist on
# the same shared compute node.
set -euo pipefail
CONF=$1; MASTER=$2; NODES=$3
VCORES=${4:-16}; MEM_MB=${5:-65536}
JOB=${SLURM_JOB_ID:-manual}
# Node-local disk.  /scratch is not mounted on every node; /tmp (node-local, ~400 GB) is.
LOCAL=${HADOOP_LOCAL_ROOT:-/tmp/$USER}/hadoop-$JOB
P=${HADOOP_PORT_BASE:-21000}       # port block: 21000-21099

mkdir -p "$CONF"
cp -r "$HADOOP_HOME"/etc/hadoop/* "$CONF"/ 2>/dev/null || true
echo "$NODES" | tr ' ' '\n' > "$CONF/workers"

prop() { printf '  <property><name>%s</name><value>%s</value></property>\n' "$1" "$2"; }
xml() { echo '<?xml version="1.0"?>'; echo '<configuration>'; cat; echo '</configuration>'; }

{
prop fs.defaultFS "hdfs://$MASTER:$((P+0))"
prop hadoop.tmp.dir "$LOCAL/tmp"
prop io.file.buffer.size 131072
} | xml > "$CONF/core-site.xml"

{
prop dfs.replication 1
prop dfs.namenode.name.dir "file://$LOCAL/nn"
prop dfs.datanode.data.dir "file://$LOCAL/dn"
prop dfs.permissions.enabled false
prop dfs.blocksize 134217728
prop dfs.namenode.http-address "$MASTER:$((P+1))"
prop dfs.namenode.secondary.http-address "$MASTER:$((P+2))"
prop dfs.datanode.address "0.0.0.0:$((P+3))"
prop dfs.datanode.http.address "0.0.0.0:$((P+4))"
prop dfs.datanode.ipc.address "0.0.0.0:$((P+5))"
prop dfs.namenode.safemode.extension 0
prop dfs.namenode.handler.count 32
} | xml > "$CONF/hdfs-site.xml"

{
prop yarn.resourcemanager.hostname "$MASTER"
prop yarn.resourcemanager.address "$MASTER:$((P+10))"
prop yarn.resourcemanager.scheduler.address "$MASTER:$((P+11))"
prop yarn.resourcemanager.resource-tracker.address "$MASTER:$((P+12))"
prop yarn.resourcemanager.admin.address "$MASTER:$((P+13))"
prop yarn.resourcemanager.webapp.address "$MASTER:$((P+14))"
prop yarn.nodemanager.address "0.0.0.0:$((P+20))"
prop yarn.nodemanager.localizer.address "0.0.0.0:$((P+21))"
prop yarn.nodemanager.webapp.address "0.0.0.0:$((P+22))"
prop mapreduce.shuffle.port "$((P+23))"
prop yarn.nodemanager.aux-services mapreduce_shuffle
prop yarn.nodemanager.resource.memory-mb "$MEM_MB"
prop yarn.nodemanager.resource.cpu-vcores "$VCORES"
prop yarn.scheduler.minimum-allocation-mb 512
prop yarn.scheduler.maximum-allocation-mb "$MEM_MB"
prop yarn.scheduler.maximum-allocation-vcores "$VCORES"
prop yarn.nodemanager.vmem-check-enabled false
prop yarn.nodemanager.pmem-check-enabled false
prop yarn.nodemanager.local-dirs "$LOCAL/nm-local"
prop yarn.nodemanager.log-dirs "$LOCAL/nm-logs"
prop yarn.log-aggregation-enable false
prop yarn.nodemanager.delete.debug-delay-sec 600
# A nearly full local disk otherwise makes the NodeManager refuse all work.
prop yarn.nodemanager.disk-health-checker.max-disk-utilization-per-disk-percentage 99.0
prop yarn.nodemanager.disk-health-checker.min-free-space-per-disk-mb 1024
# CapacityScheduler counts vcores only with the DominantResourceCalculator;
# without it every container costs 1 vcore regardless of what it asks for,
# which is fine here but we set it so the vcore numbers in the UI are honest.
prop yarn.scheduler.capacity.resource-calculator org.apache.hadoop.yarn.util.resource.DominantResourceCalculator
prop yarn.application.classpath "$HADOOP_HOME/etc/hadoop,$HADOOP_HOME/share/hadoop/common/*,$HADOOP_HOME/share/hadoop/common/lib/*,$HADOOP_HOME/share/hadoop/hdfs/*,$HADOOP_HOME/share/hadoop/hdfs/lib/*,$HADOOP_HOME/share/hadoop/mapreduce/*,$HADOOP_HOME/share/hadoop/yarn/*,$HADOOP_HOME/share/hadoop/yarn/lib/*"
} | xml > "$CONF/yarn-site.xml"

{
prop mapreduce.framework.name yarn
prop yarn.app.mapreduce.am.env "HADOOP_MAPRED_HOME=$HADOOP_HOME"
prop mapreduce.map.env "HADOOP_MAPRED_HOME=$HADOOP_HOME"
prop mapreduce.reduce.env "HADOOP_MAPRED_HOME=$HADOOP_HOME"
prop mapreduce.application.classpath "$HADOOP_HOME/share/hadoop/mapreduce/*,$HADOOP_HOME/share/hadoop/mapreduce/lib/*,$HADOOP_HOME/share/hadoop/common/*,$HADOOP_HOME/share/hadoop/common/lib/*,$HADOOP_HOME/share/hadoop/hdfs/*,$HADOOP_HOME/share/hadoop/hdfs/lib/*,$HADOOP_HOME/share/hadoop/yarn/*,$HADOOP_HOME/share/hadoop/yarn/lib/*"
prop yarn.app.mapreduce.am.resource.mb 2048
prop mapreduce.map.memory.mb 2048
prop mapreduce.reduce.memory.mb 3072
prop mapreduce.map.java.opts -Xmx1536m
prop mapreduce.reduce.java.opts -Xmx2560m
prop mapreduce.task.io.sort.mb 256
prop mapreduce.map.speculative false
prop mapreduce.reduce.speculative false
prop mapreduce.jobhistory.address "$MASTER:$((P+30))"
prop mapreduce.jobhistory.webapp.address "$MASTER:$((P+31))"
prop mapreduce.cluster.local.dir "$LOCAL/mr-local"
} | xml > "$CONF/mapred-site.xml"

cat >> "$CONF/hadoop-env.sh" <<EOT
export JAVA_HOME=$JAVA_HOME
export HADOOP_LOG_DIR=$LOCAL/logs
export HADOOP_PID_DIR=$LOCAL/pids
export HADOOP_HEAPSIZE_MAX=4g
export HADOOP_OS_TYPE=\${HADOOP_OS_TYPE:-\$(uname -s)}
EOT
cat >> "$CONF/yarn-env.sh" <<EOT
export YARN_LOG_DIR=$LOCAL/logs
export YARN_PID_DIR=$LOCAL/pids
EOT
echo "config written to $CONF (master=$MASTER nodes='$NODES' vcores=$VCORES mem=${MEM_MB}MB local=$LOCAL)"
