# Source this on the cluster (login node or compute node) to get Hadoop 3.3.6.
#   source ~/HW3/hadoop/env.sh
export HADOOP_HOME=${HADOOP_HOME:-$HOME/apps/hadoop-3.3.6}
export JAVA_HOME=${JAVA_HOME:-/usr/local/apps/jdk-11.0.13+8}
[ -d "$JAVA_HOME" ] || export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
# The live cluster's configuration.  cluster.sbatch points this symlink at the
# per-job config directory it generates, so any shell that sources env.sh talks
# to whichever cluster is currently up.
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-$HOME/HW3/hadoop/conf-live}
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export HADOOP_YARN_HOME=$HADOOP_HOME
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$JAVA_HOME/bin:$PATH
export STREAMING_JAR=$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar
